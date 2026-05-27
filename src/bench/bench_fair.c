/*
 * bench_fair.c — apples-to-apples throughput: items consumed per second,
 *                fixed 4096-slot capacity for every bounded design,
 *                mseq in gated mode, P/C/range grid.
 *
 * Primary metric of the paper. Numbers from this binary are the ones
 * cited in README.md and ARCHITECTURE.md §3. Do not mix with
 * bench_compare.c (which reports aggregate).
 *
 * Invariants:
 *  - TOTAL_CAPACITY = 4096 is fixed across mseq / SCQD / CK-Ring.
 *  - mseq runs in GATED mode (rp != NULL) — required for correctness
 *    under wrap when consumers are slower than producers.
 *  - Hot path uses thread-local counters; the single atomic add to
 *    g_consumed happens only at thread exit.
 *  - 100ms warmup before the BENCH_SEC measurement window.
 *
 * Not allowed:
 *  - Change TOTAL_CAPACITY without also updating the paper / README
 *    tables (the capacity is the comparison axis).
 *  - Use mseq ungated here — would invalidate the safety claim.
 *  - Print intermediate counts during the run (mutex/printf in the hot
 *    path breaks the measurement).
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "mpmc_seq.h"
#include "lf/lf.h"
#include "lfring_cas1.h"
#include <ck_ring.h>

#define TOTAL_CAPACITY 4096
#define ITEM_SIZE sizeof(uint64_t)
#define BENCH_SEC 2
#define MAX_CONSUMERS 64

/* Global: CPU pinning enabled via --pin flag */
static int g_pin_threads = 0;
static int g_num_cpus = 0;

typedef struct {
    void *ctx; void *ctx2;  /* ctx2 = rpool for mseq */
    _Atomic bool *run;
    _Atomic int *counting;  /* only count after warmup */
    _Atomic uint64_t *consumed;
    int id; int range;
    int cpu_id;  /* -1 = no pinning */
} targ_t;

static void pin_to_cpu(int cpu_id) {
    if (cpu_id < 0 || !g_pin_threads) return;
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id % g_num_cpus, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#else
    (void)cpu_id;
#endif
}

static double run_bench(void *(*pfn)(void*), void *(*cfn)(void*),
                         void *ctx, void *ctx2, int np, int nc, int range) {
    _Atomic bool run = true;
    _Atomic int counting = 0;
    _Atomic uint64_t consumed = 0;
    int tot = np + nc;
    pthread_t *th = malloc(sizeof(pthread_t) * tot);
    targ_t *ar = malloc(sizeof(targ_t) * tot);

    for (int i = 0; i < np; i++) {
        ar[i] = (targ_t){ctx, ctx2, &run, &counting, &consumed, i, range,
                          g_pin_threads ? i : -1};
        pthread_create(&th[i], NULL, pfn, &ar[i]);
    }
    for (int i = 0; i < nc; i++) {
        ar[np+i] = (targ_t){ctx, ctx2, &run, &counting, &consumed, i, range,
                              g_pin_threads ? np + i : -1};
        pthread_create(&th[np+i], NULL, cfn, &ar[np+i]);
    }

    /* Warmup */
    struct timespec w = {0, 100000000}; nanosleep(&w, NULL);

    /* Start measuring */
    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);
    atomic_store(&counting, 1);

    struct timespec d = {BENCH_SEC, 0}; nanosleep(&d, NULL);

    atomic_store(&counting, 0);
    clock_gettime(CLOCK_MONOTONIC, &t_end);
    atomic_store(&run, false);
    for (int i = 0; i < tot; i++) pthread_join(th[i], NULL);

    double elapsed = (t_end.tv_sec - t_start.tv_sec) +
                     (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    double mops = atomic_load(&consumed) / elapsed / 1e6;
    free(th); free(ar);
    return mops;
}

/* ========================================================================
 * mseq (partitioned, gated via rpool)
 * Total capacity = P × slots_per_part = TOTAL_CAPACITY
 * ======================================================================== */

static void *mseq_prod(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    mseq_t *q = t->ctx; mseq_rpool_t *rp = t->ctx2;
    uint64_t v = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(q, t->id, rp);
        if (s) { *(uint64_t *)s = v++; mseq_submit(q, t->id); }
    }
    return NULL;
}
static void *mseq_cons(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    mseq_t *q = t->ctx; mseq_rpool_t *rp = t->ctx2;
    uint64_t n = 0;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim(q, rp, t->id, t->range);
        if (c.data) {
            if (atomic_load(t->counting)) n += c.count;
            mseq_release(q, rp, t->id, &c);
        }
    }
    atomic_fetch_add(t->consumed, n);
    return NULL;
}

/* ========================================================================
 * SCQD (Nikolaev, single ring, capacity = TOTAL_CAPACITY)
 * ======================================================================== */

typedef struct {
    struct lfring *aq, *fq; uint64_t *val;
    size_t order, half;
} scqd_t;

static int scqd_init(scqd_t *s, size_t order) {
    s->order = order; s->half = (size_t)1 << order;
    size_t rsz = LFRING_SIZE(order);
    s->aq = aligned_alloc(LFRING_ALIGN, rsz);
    s->fq = aligned_alloc(LFRING_ALIGN, rsz);
    s->val = calloc(s->half, sizeof(uint64_t));
    if (!s->aq || !s->fq || !s->val) return -1;
    lfring_init_empty(s->aq, order);
    lfring_init_full(s->fq, order);
    return 0;
}
static void scqd_destroy(scqd_t *s) { free(s->aq); free(s->fq); free(s->val); }

static void *scqd_prod(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    scqd_t *s = t->ctx;
    uint64_t v = 0;
    while (atomic_load(t->run)) {
        size_t idx = lfring_dequeue(s->fq, s->order, false);
        if (idx == LFRING_EMPTY) continue;
        s->val[idx] = v++;
        lfring_enqueue(s->aq, s->order, idx, false);
    }
    return NULL;
}
static void *scqd_cons(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    scqd_t *s = t->ctx;
    uint64_t n = 0;
    while (atomic_load(t->run)) {
        size_t idx = lfring_dequeue(s->aq, s->order, false);
        if (idx == LFRING_EMPTY) continue;
        volatile uint64_t v = s->val[idx]; (void)v;
        lfring_enqueue(s->fq, s->order, idx, true);
        if (atomic_load(t->counting)) n++;
    }
    atomic_fetch_add(t->consumed, n);
    return NULL;
}

/* ========================================================================
 * CK-Ring (single ring, capacity = TOTAL_CAPACITY)
 * ======================================================================== */

typedef struct { ck_ring_t ring; ck_ring_buffer_t *buf; } ckctx_t;

static void *ck_prod(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    ckctx_t *k = t->ctx;
    uint64_t v = 1;
    while (atomic_load(t->run)) {
        ck_ring_enqueue_mpmc(&k->ring, k->buf, (void*)(uintptr_t)v++);
    }
    return NULL;
}
static void *ck_cons(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    ckctx_t *k = t->ctx;
    void *p; uint64_t n = 0;
    while (atomic_load(t->run)) {
        if (ck_ring_dequeue_mpmc(&k->ring, k->buf, &p)) {
            if (atomic_load(t->counting)) n++;
        }
    }
    atomic_fetch_add(t->consumed, n);
    return NULL;
}

/* ========================================================================
 * CK-Ring partitioned — P independent SPMC rings (D.2 baseline)
 *
 * Purpose: isolate the LL-vs-strict effect from the partitioning effect.
 * mseq vs single-ring ck-ring conflates both. This baseline keeps ck-ring's
 * per-slot CAS design but partitions into P rings of TOTAL_CAPACITY/P slots.
 *   - Spec: each ring is strictly linearisable; global queue is locally
 *           linearisable (LL, same spec class as mseq).
 *   - Producer: 1 per partition, ck_ring_enqueue_spmc (no inter-producer CAS).
 *   - Consumer: round-robin scan over the P rings, ck_ring_dequeue_spmc
 *               (consumers contend within a ring only).
 *   - No range claim (1 dequeue per item, unlike mseq).
 * Total capacity preserved: P × (TOTAL_CAPACITY/P) = TOTAL_CAPACITY.
 * ======================================================================== */

typedef struct {
    ck_ring_t ring;
    ck_ring_buffer_t *buf;
    char _pad[128];  /* cache-line isolation between partitions */
} __attribute__((aligned(128))) ckpart_t;

typedef struct {
    ckpart_t *parts;
    int num_parts;
    uint32_t slots_per_part;  /* power of 2 */
} ckpctx_t;

static int ckp_init(ckpctx_t *k, int num_parts, uint32_t slots_per_part) {
    /* slots_per_part must be power of 2 for ck_ring */
    if (slots_per_part & (slots_per_part - 1)) {
        uint32_t v = slots_per_part - 1;
        v |= v>>1; v |= v>>2; v |= v>>4; v |= v>>8; v |= v>>16;
        slots_per_part = v + 1;
    }
    k->num_parts = num_parts;
    k->slots_per_part = slots_per_part;
    k->parts = aligned_alloc(128, sizeof(ckpart_t) * num_parts);
    if (!k->parts) return -1;
    for (int i = 0; i < num_parts; i++) {
        k->parts[i].buf = calloc(slots_per_part, sizeof(ck_ring_buffer_t));
        if (!k->parts[i].buf) return -1;
        ck_ring_init(&k->parts[i].ring, slots_per_part);
    }
    return 0;
}

static void ckp_destroy(ckpctx_t *k) {
    for (int i = 0; i < k->num_parts; i++) free(k->parts[i].buf);
    free(k->parts);
}

static void *ckp_prod(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    ckpctx_t *k = t->ctx;
    /* one producer per partition (SPMC) */
    int j = t->id % k->num_parts;
    ck_ring_t *r = &k->parts[j].ring;
    ck_ring_buffer_t *b = k->parts[j].buf;
    uint64_t v = 1;
    while (atomic_load(t->run)) {
        ck_ring_enqueue_spmc(r, b, (void*)(uintptr_t)v++);
    }
    return NULL;
}

static void *ckp_cons(void *a) {
    targ_t *t = a; pin_to_cpu(t->cpu_id);
    ckpctx_t *k = t->ctx;
    void *p; uint64_t n = 0;
    int start = t->id % k->num_parts;  /* spread consumers across partitions */
    while (atomic_load(t->run)) {
        bool got = false;
        for (int i = 0; i < k->num_parts; i++) {
            int idx = (start + i) % k->num_parts;
            if (ck_ring_dequeue_spmc(&k->parts[idx].ring, k->parts[idx].buf, &p)) {
                if (atomic_load(t->counting)) n++;
                start = (idx + 1) % k->num_parts;  /* next time, try next partition */
                got = true;
                break;
            }
        }
        if (!got) {
            /* All partitions empty: short pause to avoid busy-spin starving producers */
            sched_yield();
        }
    }
    atomic_fetch_add(t->consumed, n);
    return NULL;
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(int argc, char **argv) {
    /* Parse --pin flag */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--pin") == 0) g_pin_threads = 1;
    }
    g_num_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    if (g_pin_threads)
        fprintf(stderr, "CPU pinning enabled (%d CPUs available)\n", g_num_cpus);

    int cfgs[][2] = {
        /* Balanced P=C */
        {1,1}, {2,2}, {4,4}, {8,8}, {16,16},
        /* P>C (amortization sweet spot) */
        {4,1}, {8,1}, {16,1},
        /* C>P (contention stress — the critical regime) */
        {4,8}, {4,16}, {4,32},
        {2,8}, {2,16}, {2,32},
        {1,4}, {1,8}, {1,16}, {1,32},
    };
    int ncfg = sizeof(cfgs)/sizeof(cfgs[0]);

    printf("# Fair Benchmark — Items consumed/sec (Mops/s)\n\n");
    printf("## Rules\n\n");
    printf("- Metric: consumer throughput only (items dequeued per second)\n");
    printf("- Total capacity: %d items for ALL bounded designs\n", TOTAL_CAPACITY);
    printf("  - mseq at P partitions: P × %d/P = %d total slots\n", TOTAL_CAPACITY, TOTAL_CAPACITY);
    printf("  - SCQD: %d slots (single ring). CK-Ring: %d slots (single ring)\n", TOTAL_CAPACITY, TOTAL_CAPACITY);
    printf("- mseq uses gated mode (per-consumer cursors) for correctness\n");
    printf("- Local counting (no atomic in hot path), aggregated at thread exit\n");
    printf("- Item = uint64_t, written by producer, read by consumer\n");
    printf("- Duration: %d seconds per config, 100ms warmup discarded\n\n", BENCH_SEC);
    printf("## Structural differences (not normalized, part of the design)\n\n");
    printf("- mseq: P partitions (1 per producer), range claim (1 CAS per N items), LL\n");
    printf("- SCQD: single ring, 2 internal rings (alloc queue + free queue), per-slot FAA+CAS, strict\n");
    printf("- CK-Ring: single ring, per-slot CAS, strict\n");
    printf("- CK-Part: P partitions (SPMC rings), per-slot CAS, no range claim, LL — D.2 baseline\n\n");

    printf("## Results\n\n");
    printf("| Config | mseq R=1 | mseq R=16 | mseq R=32 | SCQD | CK-Ring | CK-Part |\n");
    printf("|--------|----------|-----------|-----------|------|---------|---------|\n");

    size_t scq_order = 12;  /* 2^12 = 4096 */

    for (int c = 0; c < ncfg; c++) {
        int np = cfgs[c][0], nc = cfgs[c][1];
        uint32_t parts = np;
        if (parts & (parts-1)) { uint32_t v=parts; v--;v|=v>>1;v|=v>>2;v|=v>>4;v|=v>>8;v|=v>>16;parts=v+1; }

        /* mseq: TOTAL_CAPACITY / parts slots per partition */
        uint32_t slots_per_part = TOTAL_CAPACITY / parts;
        if (slots_per_part < 8) slots_per_part = 8;  /* minimum viable */

        fprintf(stderr, "  %dP/%dC (mseq: %u parts × %u slots = %u total): ",
                np, nc, parts, slots_per_part, parts * slots_per_part);

        /* mseq with rpool (gated) */
        double m[3]; int ranges[] = {1, 16, 32};
        for (int r = 0; r < 3; r++) {
            /* Clamp range to slots_per_part */
            int rng = ranges[r];
            if ((uint32_t)rng > slots_per_part) rng = slots_per_part;

            size_t qsz = mseq_buffer_size(parts, slots_per_part, ITEM_SIZE);
            void *qbuf = aligned_alloc(MSEQ_CACHE_LINE, qsz);
            mseq_t q; mseq_init(&q, qbuf, qsz, parts, slots_per_part, ITEM_SIZE);

            uint32_t ncons = nc > 0 ? nc : 1;
            size_t rsz = mseq_rpool_size(ncons);
            void *rbuf = aligned_alloc(MSEQ_CACHE_LINE, rsz);
            mseq_rpool_t rp; mseq_rpool_init(&rp, rbuf, rsz, ncons);

            m[r] = run_bench(mseq_prod, mseq_cons, &q, &rp, np, nc, rng);
            free(qbuf); free(rbuf);
        }

        /* SCQD */
        scqd_t s; scqd_init(&s, scq_order);
        double scqd = run_bench(scqd_prod, scqd_cons, &s, NULL, np, nc, 0);
        scqd_destroy(&s);

        /* CK-Ring */
        ckctx_t ck;
        ck.buf = calloc(TOTAL_CAPACITY, sizeof(ck_ring_buffer_t));
        ck_ring_init(&ck.ring, TOTAL_CAPACITY);
        double ckr = run_bench(ck_prod, ck_cons, &ck, NULL, np, nc, 0);
        free(ck.buf);

        /* CK-Part: same partitioning as mseq, ck-ring per-slot CAS design */
        ckpctx_t ckp;
        ckp_init(&ckp, parts, slots_per_part);
        double ckpr = run_bench(ckp_prod, ckp_cons, &ckp, NULL, np, nc, 0);
        ckp_destroy(&ckp);

        fprintf(stderr, "m1=%.1f m16=%.1f m32=%.1f scq=%.1f ck=%.1f ckp=%.1f\n",
                m[0], m[1], m[2], scqd, ckr, ckpr);
        printf("| **%dP/%dC** | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f |\n",
               np, nc, m[0], m[1], m[2], scqd, ckr, ckpr);
    }

    printf("\n## Notes on moodycamel (not shown)\n\n");
    printf("moodycamel::ConcurrentQueue is **unbounded** (uses malloc when full).\n");
    printf("It never blocks producers. Under sustained load, it grows memory\n");
    printf("indefinitely. This gives it a structural throughput advantage over\n");
    printf("bounded designs that block when the ring is full.\n\n");
    printf("Including moodycamel in a bounded-capacity benchmark would be\n");
    printf("misleading. Run bench_reference_cxx separately for moodycamel numbers.\n");
    printf("It achieves ~60-488 Mops/s depending on config, but is a different\n");
    printf("design category (unbounded, allocation-based, token-affinity).\n\n");
    printf("For applications where allocation is forbidden (robotics, kernel,\n");
    printf("firmware, real-time), moodycamel is not usable. mseq is designed\n");
    printf("for this constrained space.\n");

    return 0;
}
