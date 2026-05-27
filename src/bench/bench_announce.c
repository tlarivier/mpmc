/*
 * bench_announce.c — head-to-head: mseq_claim vs mseq_claim_announced
 *                    at consumer-heavy C >> P regimes where the
 *                    announcement protocol's break-even applies.
 *
 * Same fair methodology as bench_fair.c: consumer-only throughput,
 * fixed 4096-slot capacity, 2-second window.
 *
 * Invariants:
 *  - Both paths share the same rpool layout; the only call-site
 *    difference is mseq_claim vs mseq_claim_announced.
 *  - On x86_64 / WSL2 the announcement path is expected to LOSE — this
 *    binary documents the regression numerically (see
 *    ARCHITECTURE.md §11).
 *
 * Not allowed:
 *  - Mix standard and announce calls against the same rpool instance.
 *    Re-init the rpool between runs to avoid stale .part values.
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
#include "mseq_announce.h"

#define TOTAL_CAPACITY 4096
#define ITEM_SIZE sizeof(uint64_t)
#define BENCH_SEC 2

static int g_pin = 0;
static int g_ncpus = 0;

static void pin(int cpu) {
    if (!g_pin || cpu < 0) return;
#ifdef __linux__
    cpu_set_t s; CPU_ZERO(&s); CPU_SET(cpu % g_ncpus, &s);
    pthread_setaffinity_np(pthread_self(), sizeof(s), &s);
#else
    (void)cpu;
#endif
}

typedef struct {
    mseq_t *q; mseq_rpool_t *rp;
    _Atomic bool *run;
    _Atomic int *counting;
    _Atomic uint64_t *consumed;
    int id, range, cpu, use_announce;
} targ_t;

static void *producer(void *a) {
    targ_t *t = a; pin(t->cpu);
    uint64_t v = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(t->q, t->id, t->rp);
        if (s) { *(uint64_t *)s = v++; mseq_submit(t->q, t->id); }
    }
    return NULL;
}

static void *consumer_std(void *a) {
    targ_t *t = a; pin(t->cpu);
    uint64_t n = 0;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim(t->q, t->rp, t->id, t->range);
        if (c.data) {
            if (atomic_load(t->counting)) n += c.count;
            mseq_release(t->q, t->rp, t->id, &c);
        }
    }
    atomic_fetch_add(t->consumed, n);
    return NULL;
}

static void *consumer_ann(void *a) {
    targ_t *t = a; pin(t->cpu);
    uint64_t n = 0;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim_announced(t->q, t->rp, t->id, t->range);
        if (c.data) {
            if (atomic_load(t->counting)) n += c.count;
            mseq_release(t->q, t->rp, t->id, &c);
        }
    }
    atomic_fetch_add(t->consumed, n);
    return NULL;
}

static double run_bench(int np, int nc, int range, int announce) {
    uint32_t parts = np;
    if (parts & (parts-1)) { uint32_t v=parts; v--;v|=v>>1;v|=v>>2;v|=v>>4;v|=v>>8;v|=v>>16;parts=v+1; }

    uint32_t slots = TOTAL_CAPACITY / parts;
    if (slots < 8) slots = 8;

    size_t qsz = mseq_buffer_size(parts, slots, ITEM_SIZE);
    void *qbuf = aligned_alloc(MSEQ_CACHE_LINE, qsz);
    mseq_t q; mseq_init(&q, qbuf, qsz, parts, slots, ITEM_SIZE);

    uint32_t ncons = nc > 0 ? (uint32_t)nc : 1;
    size_t rsz = mseq_rpool_size(ncons);
    void *rbuf = aligned_alloc(MSEQ_CACHE_LINE, rsz);
    mseq_rpool_t rp; mseq_rpool_init(&rp, rbuf, rsz, ncons);

    _Atomic bool run = true;
    _Atomic int counting = 0;
    _Atomic uint64_t consumed = 0;
    int tot = np + nc;
    pthread_t *th = malloc(sizeof(pthread_t) * tot);
    targ_t *ar = malloc(sizeof(targ_t) * tot);

    for (int i = 0; i < np; i++) {
        ar[i] = (targ_t){&q, &rp, &run, &counting, &consumed, i, range,
                          g_pin ? i : -1, announce};
        pthread_create(&th[i], NULL, producer, &ar[i]);
    }
    void *(*cfn)(void*) = announce ? consumer_ann : consumer_std;
    for (int i = 0; i < nc; i++) {
        ar[np+i] = (targ_t){&q, &rp, &run, &counting, &consumed, i, range,
                              g_pin ? np + i : -1, announce};
        pthread_create(&th[np+i], NULL, cfn, &ar[np+i]);
    }

    struct timespec w = {0, 100000000}; nanosleep(&w, NULL);
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
    free(th); free(ar); free(qbuf); free(rbuf);
    return mops;
}

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++)
        if (strcmp(argv[i], "--pin") == 0) g_pin = 1;
    g_ncpus = sysconf(_SC_NPROCESSORS_ONLN);

    int cfgs[][2] = {
        {1,1}, {2,2}, {4,4}, {8,8},
        {4,1},
        {4,8}, {4,16}, {4,32},
        {2,8}, {2,16}, {2,32},
        {1,4}, {1,8}, {1,16}, {1,32},
    };
    int ncfg = sizeof(cfgs)/sizeof(cfgs[0]);
    int range = 32;

    printf("# Announcement Protocol Benchmark (R=%d, %d CPUs%s)\n\n",
           range, g_ncpus, g_pin ? ", pinned" : "");
    printf("| Config | C/P | Standard | Announced | Speedup |\n");
    printf("|--------|:---:|:--------:|:---------:|:-------:|\n");

    for (int i = 0; i < ncfg; i++) {
        int np = cfgs[i][0], nc = cfgs[i][1];
        fprintf(stderr, "  %dP/%dC...", np, nc);

        double std = run_bench(np, nc, range, 0);
        double ann = run_bench(np, nc, range, 1);
        double speedup = std > 0 ? ann / std : 0;

        printf("| **%dP/%dC** | %d | %.1f | %.1f | %.2fx |\n",
               np, nc, nc/np, std, ann, speedup);
        fprintf(stderr, " std=%.1f ann=%.1f (%.2fx)\n", std, ann, speedup);
    }

    return 0;
}
