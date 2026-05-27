/*
 * bench_compare.c — mseq vs CK-Ring, enqueues and dequeues counted
 *                   together, 2 s per configuration.
 *
 * Invariants:
 *  - CAPACITY (4096) slots in total for both queues. mseq splits them
 *    across partitions.
 *  - mseq runs gated and its consumers read their items.
 *
 * Not allowed:
 *  - Comparing these numbers with bench_fair (consumer-only metric).
 *  - Adding raw SCQ: its enqueue spins on a full ring and the stop flag
 *    can't end the run. Use bench_scq.c.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "mpmc_seq.h"
#include "bench_common.h"
#include <ck_ring.h>

#define CAPACITY 4096
#define ITEM_SIZE sizeof(uint64_t)
#define BENCH_SEC 2

typedef struct { void *ctx; _Atomic bool *run; _Atomic int *counting; _Atomic uint64_t *cnt; int id; int range; } targ_t;

static void *mseq_prod(void *a) {
    targ_t *t = a; mseq_bench_t *b = t->ctx; uint64_t v = 0, n = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(&b->q, t->id, &b->rp);
        if (s) { *(uint64_t *)s = v++; mseq_submit(&b->q, t->id); if (atomic_load(t->counting)) n++; }
    }
    atomic_fetch_add(t->cnt, n); return NULL;
}
static void *mseq_cons(void *a) {
    targ_t *t = a; mseq_bench_t *b = t->ctx; uint64_t n = 0, sum = 0;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim(&b->q, &b->rp, t->id, t->range);
        if (c.data) {
            sum += mseq_bench_read(&b->q, &c);
            if (atomic_load(t->counting)) n += c.count;
            mseq_release(&b->q, &b->rp, t->id, &c);
        }
    }
    mseq_bench_sink(sum);
    atomic_fetch_add(t->cnt, n); return NULL;
}
static void *ck_prod(void *a) {
    targ_t *t = a; void **ck = t->ctx; ck_ring_t *r = ck[0]; ck_ring_buffer_t *b = ck[1];
    uint64_t v = 1, n = 0;
    while (atomic_load(t->run)) { if (ck_ring_enqueue_mpmc(r, b, (void*)(uintptr_t)v++)) { if (atomic_load(t->counting)) n++; } }
    atomic_fetch_add(t->cnt, n); return NULL;
}
static void *ck_cons(void *a) {
    targ_t *t = a; void **ck = t->ctx; ck_ring_t *r = ck[0]; ck_ring_buffer_t *b = ck[1];
    uint64_t n = 0; void *p;
    while (atomic_load(t->run)) { if (ck_ring_dequeue_mpmc(r, b, &p)) { if (atomic_load(t->counting)) n++; } }
    atomic_fetch_add(t->cnt, n); return NULL;
}

typedef struct { struct lfring *ring; size_t order; } scqctx_t;

typedef void *(*fn_t)(void *);

static double run(fn_t pfn, fn_t cfn, void *ctx, int np, int nc, int range) {
    _Atomic bool run = true; _Atomic int counting = 0; _Atomic uint64_t cnt = 0;
    int tot = np + nc;
    pthread_t *th = malloc(sizeof(pthread_t) * tot);
    targ_t *ar = malloc(sizeof(targ_t) * tot);
    for (int i = 0; i < np; i++) { ar[i]    = (targ_t){ctx, &run, &counting, &cnt, i, range}; pthread_create(&th[i],    NULL, pfn, &ar[i]);    }
    for (int i = 0; i < nc; i++) { ar[np+i] = (targ_t){ctx, &run, &counting, &cnt, i, range}; pthread_create(&th[np+i], NULL, cfn, &ar[np+i]); }
    struct timespec w = {0, 100000000}; nanosleep(&w, NULL);
    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);
    atomic_store(&counting, 1);
    struct timespec d = {BENCH_SEC, 0}; nanosleep(&d, NULL);
    atomic_store(&counting, 0);
    clock_gettime(CLOCK_MONOTONIC, &t_end);
    atomic_store(&run, false);
    for (int i = 0; i < tot; i++) pthread_join(th[i], NULL);
    double elapsed = (t_end.tv_sec - t_start.tv_sec) + (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    double mops = atomic_load(&cnt) / elapsed / 1e6;
    free(th); free(ar);
    return mops;
}

int main(void) {
    int cfgs[][2] = {{1,1},{2,2},{4,4},{8,8},{16,16},{4,1},{4,8},{1,4},{1,8}};
    int nc = sizeof(cfgs)/sizeof(cfgs[0]);
    int ranges[] = {1, 4, 16, 32};
    int nr = sizeof(ranges)/sizeof(ranges[0]);

    printf("# mseq vs CK-Ring (Capacity=%d, CacheLine=%d)\n\n", CAPACITY, MSEQ_CACHE_LINE);
    printf("| Config |");
    for (int r = 0; r < nr; r++) printf(" mseq R=%d |", ranges[r]);
    printf(" CK-Ring |\n|--------|");
    for (int r = 0; r < nr; r++) printf("----------|");
    printf("---------|\n");

    for (int c = 0; c < nc; c++) {
        int np = cfgs[c][0], ncons = cfgs[c][1];
        uint32_t parts = mseq_bench_pow2(np);

        printf("| **%dP/%dC** |", np, ncons);
        fprintf(stderr, "  %dP/%dC: ", np, ncons);

        for (int r = 0; r < nr; r++) {
            mseq_bench_t b;
            if (mseq_bench_init(&b, parts, CAPACITY, ncons)) { fprintf(stderr, "init failed\n"); return 1; }
            double m = run(mseq_prod, mseq_cons, &b, np, ncons, ranges[r]);
            mseq_bench_free(&b);
            printf(" %.1f |", m);
            fprintf(stderr, "R%d=%.1f ", ranges[r], m);
        }

        ck_ring_t ring; ck_ring_buffer_t *ckbuf = calloc(CAPACITY, sizeof(ck_ring_buffer_t));
        ck_ring_init(&ring, CAPACITY);
        void *ckctx[2] = {&ring, ckbuf};
        double ckm = run(ck_prod, ck_cons, ckctx, np, ncons, 0);
        free(ckbuf);
        printf(" %.1f |\n", ckm);
        fprintf(stderr, "CK=%.1f\n", ckm);
    }
    return 0;
}
