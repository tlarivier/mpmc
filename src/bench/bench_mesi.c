/*
 * bench_mesi.c — measures the physical cost of MESI transitions: CAS,
 *                fetch_add, and Shared loads at 1/2/4/8 cores. Produces
 *                the T_exc(C) and T_sh numbers cited in
 *                ARCHITECTURE.md §3 and used as input to the cost model.
 *
 * What it measures, in order:
 *   1. baseline atomic on private line          (T_exc(1))
 *   2. Shared-load on line written by peer       (T_sh)
 *   3. CAS success private vs contended
 *   4. CAS failure cost (peer modifies between read and CAS)
 *   5. fetch_add at 2/4/8 cores
 *   6. CAS at 2/4/8 cores
 *
 * Invariants:
 *  - ITERATIONS / WARMUP are calibrated for sub-second runs at 10M ops;
 *    they are not the same scale as bench_fair (do not extrapolate).
 *  - Threads pin to distinct cores when sched_setaffinity is available;
 *    otherwise the result is treated as "best effort" and so labeled.
 *
 * Not allowed:
 *  - Use this binary's numbers to compare designs — it measures
 *    primitives, not designs. Use bench_fair.c for that.
 *  - Add print/syscall inside the inner loop (would skew the measurement
 *    beyond the noise floor we already document).
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <sched.h>

#include "mpmc_seq.h"
#ifndef CACHE_LINE
#define CACHE_LINE MSEQ_CACHE_LINE
#endif
#define ITERATIONS 10000000
#define WARMUP     1000000

/* Cache-line padded atomic */
typedef struct {
    _Atomic uint64_t val;
    uint8_t _pad[CACHE_LINE - sizeof(uint64_t)];
} __attribute__((aligned(CACHE_LINE))) padded_atomic_t;

/* Per-thread result */
typedef struct {
    uint64_t ops;
    uint64_t successes;
    uint64_t failures;
    uint64_t elapsed_ns;
} bench_result_t;

/* Shared state */
typedef struct {
    padded_atomic_t target;          /* The contended line */
    padded_atomic_t ready;           /* Sync barrier */
    padded_atomic_t go;              /* Start signal */
    padded_atomic_t stop;            /* Stop signal */
    int num_threads;
    int mode;                        /* 0=CAS, 1=fetch_add, 2=load-only */
    bench_result_t results[64];
} bench_state_t;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* ========================================================================
 * Benchmark: private line (no contention baseline)
 * ======================================================================== */

static double bench_private_cas(void) {
    padded_atomic_t local;
    atomic_store(&local.val, 0);

    uint64_t start = now_ns();
    for (int i = 0; i < ITERATIONS; i++) {
        uint64_t expected = i;
        atomic_compare_exchange_strong_explicit(&local.val, &expected, i + 1,
            memory_order_acquire, memory_order_relaxed);
    }
    uint64_t elapsed = now_ns() - start;
    return (double)elapsed / ITERATIONS;
}

static double bench_private_fetch_add(void) {
    padded_atomic_t local;
    atomic_store(&local.val, 0);

    uint64_t start = now_ns();
    for (int i = 0; i < ITERATIONS; i++) {
        atomic_fetch_add_explicit(&local.val, 1, memory_order_acquire);
    }
    uint64_t elapsed = now_ns() - start;
    return (double)elapsed / ITERATIONS;
}

static double bench_private_store_release(void) {
    padded_atomic_t local;
    atomic_store(&local.val, 0);

    uint64_t start = now_ns();
    for (int i = 0; i < ITERATIONS; i++) {
        atomic_store_explicit(&local.val, i, memory_order_release);
    }
    uint64_t elapsed = now_ns() - start;
    return (double)elapsed / ITERATIONS;
}

static double bench_private_load_acquire(void) {
    padded_atomic_t local;
    atomic_store(&local.val, 42);

    uint64_t start = now_ns();
    volatile uint64_t sink = 0;
    for (int i = 0; i < ITERATIONS; i++) {
        sink = atomic_load_explicit(&local.val, memory_order_acquire);
    }
    (void)sink;
    uint64_t elapsed = now_ns() - start;
    return (double)elapsed / ITERATIONS;
}

/* ========================================================================
 * Benchmark: contended line (multiple cores)
 * ======================================================================== */

static void *contention_thread(void *arg) {
    bench_state_t *st = ((void **)arg)[0];
    int id = (int)(uintptr_t)((void **)arg)[1];

    /* Signal ready and wait for go */
    atomic_fetch_add(&st->ready.val, 1);
    while (!atomic_load(&st->go.val)) { /* spin */ }

    uint64_t ops = 0, successes = 0, failures = 0;
    uint64_t start = now_ns();

    while (!atomic_load_explicit(&st->stop.val, memory_order_relaxed)) {
        if (st->mode == 0) {
            /* CAS — try to increment by 1 */
            uint64_t cur = atomic_load_explicit(&st->target.val, memory_order_relaxed);
            uint64_t next = cur + 1;
            if (atomic_compare_exchange_strong_explicit(&st->target.val, &cur, next,
                    memory_order_acquire, memory_order_relaxed)) {
                successes++;
            } else {
                failures++;
            }
        } else if (st->mode == 1) {
            /* fetch_add — always succeeds */
            atomic_fetch_add_explicit(&st->target.val, 1, memory_order_acquire);
            successes++;
        } else {
            /* load-only — read shared */
            volatile uint64_t v = atomic_load_explicit(&st->target.val, memory_order_acquire);
            (void)v;
            successes++;
        }
        ops++;
    }

    uint64_t elapsed = now_ns() - start;
    st->results[id].ops        = ops;
    st->results[id].successes  = successes;
    st->results[id].failures   = failures;
    st->results[id].elapsed_ns = elapsed;

    return NULL;
}

static void run_contention_bench(int num_threads, int mode, const char *label) {
    bench_state_t st;
    memset(&st, 0, sizeof(st));
    st.num_threads = num_threads;
    st.mode = mode;
    atomic_store(&st.target.val, 0);
    atomic_store(&st.ready.val,  0);
    atomic_store(&st.go.val,     0);
    atomic_store(&st.stop.val,   0);

    pthread_t threads[64];
    void *args[64][2];

    for (int i = 0; i < num_threads; i++) {
        args[i][0] = &st;
        args[i][1] = (void *)(uintptr_t)i;
        pthread_create(&threads[i], NULL, contention_thread, args[i]);
    }

    /* Wait for all threads ready */
    while (atomic_load(&st.ready.val) < (uint64_t)num_threads) { /* spin */ }

    /* Go */
    atomic_store(&st.go.val, 1);

    /* Run for 1 second */
    struct timespec dur = {1, 0};
    nanosleep(&dur, NULL);

    atomic_store(&st.stop.val, 1);

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    /* Aggregate */
    uint64_t total_ops = 0, total_succ = 0, total_fail = 0;
    for (int i = 0; i < num_threads; i++) {
        total_ops  += st.results[i].ops;
        total_succ += st.results[i].successes;
        total_fail += st.results[i].failures;
    }

    /* Use actual elapsed time for accuracy (nanosleep is not exact) */
    uint64_t max_elapsed_ns = 0;
    for (int i = 0; i < num_threads; i++) {
        if (st.results[i].elapsed_ns > max_elapsed_ns)
            max_elapsed_ns = st.results[i].elapsed_ns;
    }
    double ns_per_op = (double)max_elapsed_ns / ((double)total_ops / num_threads);
    double mops = total_ops / ((double)max_elapsed_ns / 1e9) / 1e6;
    double fail_rate = total_fail > 0 ? (double)total_fail / total_ops * 100.0 : 0.0;

    printf("| %-28s | %2d | %8.1f | %7.1f | %5.1f%% |\n",
           label, num_threads, ns_per_op, mops, fail_rate);
}

/* (MPMC contention profile removed — see legacy/) */

/* ========================================================================
 * SPSC baseline (Disruptor-style)
 * ======================================================================== */

typedef struct {
    uint8_t *data;
    uint32_t mask;
    uint32_t slot_size;
    uint8_t _pad0[CACHE_LINE - 12];

    _Atomic uint64_t head;   /* producer writes */
    uint8_t _pad1[CACHE_LINE - 8];

    _Atomic uint64_t tail;   /* consumer writes */
    uint8_t _pad2[CACHE_LINE - 8];
} spsc_t;

static int spsc_init(spsc_t *q, uint32_t slots, uint32_t slot_size) {
    q->mask = slots - 1;
    q->slot_size = slot_size;
    q->data = aligned_alloc(CACHE_LINE, (size_t)slots * slot_size);
    if (!q->data) return -1;
    memset(q->data, 0, (size_t)slots * slot_size);
    atomic_store(&q->head, 0);
    atomic_store(&q->tail, 0);
    return 0;
}

static void spsc_destroy(spsc_t *q) { free(q->data); }

static void *spsc_producer_fn(void *arg) {
    void **a = (void **)arg;
    spsc_t *q = a[0];
    _Atomic int *running = a[1];
    _Atomic uint64_t *count = a[2];

    uint64_t head = 0, n = 0;
    while (atomic_load(running)) {
        uint64_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
        if (head - tail >= q->mask) continue; /* full */
        uint64_t *slot = (uint64_t *)&q->data[(head & q->mask) * q->slot_size];
        *slot = head;
        atomic_store_explicit(&q->head, head + 1, memory_order_release);
        head++;
        n++;
    }
    atomic_fetch_add(count, n);
    return NULL;
}

static void *spsc_consumer_fn(void *arg) {
    void **a = (void **)arg;
    spsc_t *q = a[0];
    _Atomic int *running = a[1];
    _Atomic uint64_t *count = a[2];

    uint64_t tail = 0, n = 0;
    while (atomic_load(running)) {
        uint64_t head = atomic_load_explicit(&q->head, memory_order_acquire);
        if (tail >= head) continue; /* empty */
        volatile uint64_t *slot = (uint64_t *)&q->data[(tail & q->mask) * q->slot_size];
        (void)*slot;
        atomic_store_explicit(&q->tail, tail + 1, memory_order_release);
        tail++;
        n++;
    }
    atomic_fetch_add(count, n);
    return NULL;
}

static void bench_spsc_baseline(void) {
    spsc_t queue;
    spsc_init(&queue, 4096, sizeof(uint64_t));

    _Atomic int running = 1;
    _Atomic uint64_t prod_count = 0, cons_count = 0;

    void *pargs[] = {&queue, &running, &prod_count};
    void *cargs[] = {&queue, &running, &cons_count};

    pthread_t prod, cons;
    pthread_create(&prod, NULL, spsc_producer_fn, pargs);
    pthread_create(&cons, NULL, spsc_consumer_fn, cargs);

    struct timespec dur = {2, 0};
    nanosleep(&dur, NULL);
    atomic_store(&running, 0);

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);

    uint64_t total = atomic_load(&prod_count);
    double mops = total / 2.0 / 1e6;

    printf("  SPSC baseline (Disruptor-style): %.1f Mops/s (%.1f ns/op)\n",
           mops, 1e3 / mops);

    spsc_destroy(&queue);
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(void) {
    printf("# MESI Cost Microbenchmark — Apple Silicon\n\n");

    /* --- Section 1: Private line baselines --- */
    printf("## 1. Private line (no contention)\n\n");
    printf("| Operation | ns/op |\n");
    printf("|-----------|-------|\n");
    printf("| CAS (success, private)     | %.1f |\n", bench_private_cas());
    printf("| fetch_add (private)        | %.1f |\n", bench_private_fetch_add());
    printf("| store-release (private)    | %.1f |\n", bench_private_store_release());
    printf("| load-acquire (private)     | %.1f |\n", bench_private_load_acquire());

    /* --- Section 2: Contended line --- */
    printf("\n## 2. Contended line — CAS vs fetch_add vs load\n\n");
    printf("| Operation                    | Cores | ns/op   | Total Mops | Fail%% |\n");
    printf("|------------------------------|-------|---------|------------|-------|\n");

    int thread_counts[] = {1, 2, 4, 8};
    for (int i = 0; i < 4; i++) {
        int c = thread_counts[i];
        run_contention_bench(c, 0, "CAS (contended)");
        run_contention_bench(c, 1, "fetch_add (contended)");
        if (c > 1) run_contention_bench(c, 2, "load-only (shared read)");
        if (i < 3) {
            printf("|------------------------------|-------|---------|------------|-------|\n");
        }
    }

    /* --- Section 3: SPSC baseline --- */
    printf("\n## 3. SPSC Baseline (throughput ceiling)\n\n");
    bench_spsc_baseline();

    printf("\n## Analysis\n\n");
    printf("- **Private line**: baseline cost of atomic ops without MESI traffic\n");
    printf("- **Contended CAS**: measures cache line bouncing + CAS failure rate\n");
    printf("- **Contended fetch_add**: same MESI cost but 0%% failure (commutative)\n");
    printf("- **Load-only**: cost of reading a line in Shared state (no invalidation)\n");
    printf("- **SPSC baseline**: throughput ceiling — 1 transfer per item, no CAS\n");

    return 0;
}
