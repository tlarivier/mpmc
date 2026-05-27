/*
 * bench_latency.c — per-claim latency distribution (p50 / p99 / p999),
 *                   100k samples after 10k warmup, hardware-counter
 *                   timestamps (rdtsc on x86, cntvct_el0 on aarch64).
 *
 * Reports tail latency — the metric that matters for safety-critical
 * use cases (robotics, RT control) where p99.9 dictates jitter budget.
 *
 * Invariants:
 *  - Sample buffer is pre-allocated; no malloc in the measurement loop.
 *  - The producer and consumer run on separate threads; the timer is
 *    read only on the consumer side around mseq_claim/release.
 *  - cntvct_el0 frequency is read from the CPU once at startup and
 *    used to convert ticks to ns.
 *
 * Not allowed:
 *  - Add printf or any system call inside the timed region.
 *  - Use the announcement variant here — this binary calibrates the
 *    baseline mseq_claim path. Add a separate bench for announcements.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "mpmc_seq.h"

#define CAPACITY 4096
#define ITEM_SIZE sizeof(uint64_t)
#define SAMPLES 100000
#define WARMUP 10000

/* High-resolution timer (ARM64) */
static inline uint64_t rdtsc(void) {
#if defined(__aarch64__)
    uint64_t val;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(val));
    return val;
#elif defined(__x86_64__)
    uint32_t lo, hi;
    __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
#endif
}

static inline double ticks_to_ns(void) {
#if defined(__aarch64__)
    uint64_t freq;
    __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(freq));
    return 1e9 / (double)freq;
#else
    /* Calibrate: measure 10ms worth of ticks */
    uint64_t t0 = rdtsc();
    struct timespec ts = {0, 10000000};
    nanosleep(&ts, NULL);
    uint64_t t1 = rdtsc();
    return 10000000.0 / (double)(t1 - t0);
#endif
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t *)a, vb = *(const uint64_t *)b;
    return (va > vb) - (va < vb);
}

/* ========================================================================
 * Latency measurement: producer feeds, consumer measures claim latency
 * ======================================================================== */

typedef struct {
    mseq_t *q;
    mseq_rpool_t *rp;
    _Atomic bool *run;
    int id;
} targ_t;

static void *lat_producer(void *arg) {
    targ_t *t = arg;
    uint64_t v = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(t->q, t->id, t->rp);
        if (s) { *(uint64_t *)s = v++; mseq_submit(t->q, t->id); }
    }
    return NULL;
}

/* Background consumer: drains queue so consumer 0 can measure latency */
static void *lat_bg_consumer(void *arg) {
    targ_t *t = arg;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim(t->q, t->rp, t->id, 16);
        if (c.data) mseq_release(t->q, t->rp, t->id, &c);
    }
    return NULL;
}

static void measure_latency(mseq_t *q, mseq_rpool_t *rp, int np, int nc,
                             int range, const char *label) {
    _Atomic bool run = true;
    int tot_prod = np;
    pthread_t prods[64];
    targ_t pargs[64];
    pthread_t bg_cons[64];
    targ_t cargs[64];
    int num_bg_cons = nc > 1 ? nc - 1 : 0;

    for (int i = 0; i < tot_prod; i++) {
        pargs[i] = (targ_t){q, rp, &run, i};
        pthread_create(&prods[i], NULL, lat_producer, &pargs[i]);
    }

    /* Launch background consumers (consumer IDs 1..nc-1) */
    for (int i = 0; i < num_bg_cons; i++) {
        cargs[i] = (targ_t){q, rp, &run, i + 1};
        pthread_create(&bg_cons[i], NULL, lat_bg_consumer, &cargs[i]);
    }

    /* Warmup */
    for (int i = 0; i < WARMUP; i++) {
        mseq_claim_t c = mseq_claim(q, rp, 0, range);
        if (c.data) mseq_release(q, rp, 0, &c);
    }

    /* Measure */
    double ns_per_tick = ticks_to_ns();
    uint64_t *latencies = malloc(SAMPLES * sizeof(uint64_t));
    int collected = 0;

    while (collected < SAMPLES) {
        uint64_t t0 = rdtsc();
        mseq_claim_t c = mseq_claim(q, rp, 0, range);
        uint64_t t1 = rdtsc();

        if (c.data) {
            latencies[collected++] = (uint64_t)((t1 - t0) * ns_per_tick);
            mseq_release(q, rp, 0, &c);
        }
    }

    atomic_store(&run, false);
    for (int i = 0; i < tot_prod; i++) pthread_join(prods[i], NULL);
    for (int i = 0; i < num_bg_cons; i++) pthread_join(bg_cons[i], NULL);

    /* Sort and compute percentiles */
    qsort(latencies, SAMPLES, sizeof(uint64_t), cmp_u64);

    uint64_t p50  = latencies[SAMPLES / 2];
    uint64_t p90  = latencies[(int)(SAMPLES * 0.90)];
    uint64_t p99  = latencies[(int)(SAMPLES * 0.99)];
    uint64_t p999 = latencies[(int)(SAMPLES * 0.999)];
    uint64_t pmax = latencies[SAMPLES - 1];

    printf("| %-20s | %5llu | %5llu | %5llu | %6llu | %7llu |\n",
           label,
           (unsigned long long)p50, (unsigned long long)p90,
           (unsigned long long)p99, (unsigned long long)p999,
           (unsigned long long)pmax);

    free(latencies);
}

int main(void) {
    printf("# Claim Latency (ns) — %d samples per config\n\n", SAMPLES);
    printf("| Config               |   p50 |   p90 |   p99 |   p999 |     max |\n");
    printf("|----------------------|-------|-------|-------|--------|--------|\n");

    struct { int np; int nc; int range; const char *label; } cfgs[] = {
        {1, 1,  1, "1P/1C R=1"},
        {1, 1, 16, "1P/1C R=16"},
        {4, 1,  1, "4P/1C R=1"},
        {4, 1, 16, "4P/1C R=16"},
        {4, 4,  1, "4P/4C R=1"},
        {4, 4, 16, "4P/4C R=16"},
        {8, 8,  1, "8P/8C R=1"},
        {8, 8, 16, "8P/8C R=16"},
    };
    int ncfg = sizeof(cfgs) / sizeof(cfgs[0]);

    for (int c = 0; c < ncfg; c++) {
        int np = cfgs[c].np, nc = cfgs[c].nc, range = cfgs[c].range;
        uint32_t parts = np;
        if (parts & (parts - 1)) { uint32_t v = parts; v--; v|=v>>1; v|=v>>2; v|=v>>4; v|=v>>8; v|=v>>16; parts = v + 1; }

        size_t qsz = mseq_buffer_size(parts, CAPACITY, ITEM_SIZE);
        void *qbuf = aligned_alloc(MSEQ_CACHE_LINE, qsz);
        mseq_t q;
        mseq_init(&q, qbuf, qsz, parts, CAPACITY, ITEM_SIZE);

        size_t rsz = mseq_rpool_size(nc > 0 ? nc : 1);
        void *rbuf = aligned_alloc(MSEQ_CACHE_LINE, rsz);
        mseq_rpool_t rp;
        mseq_rpool_init(&rp, rbuf, rsz, nc > 0 ? nc : 1);

        measure_latency(&q, &rp, np, nc, range, cfgs[c].label);

        free(qbuf);
        free(rbuf);
    }

    printf("\n## Notes\n\n");
    printf("- Latency = time of a single mseq_claim() call (successful claims only)\n");
    printf("- Producer threads keep the queue fed continuously\n");
    printf("- Consumer 0 measures; other consumers (if any) run in background\n");
    printf("- Timer: cntvct_el0 on ARM64, rdtsc on x86\n");

    return 0;
}
