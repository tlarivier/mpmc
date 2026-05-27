/*
 * bench_compare.c — head-to-head throughput: mseq vs CK-Ring at fixed
 *                   capacity (4096 slots), 2-second runs, P/C grids.
 *
 * Reports aggregate throughput (producer writes + consumer reads),
 * NOT consumer-only — see bench_fair.c for the paper's primary metric.
 * Numbers from this binary are not directly comparable to bench_fair.
 *
 * Invariants:
 *  - mseq is exercised in UNGATED mode (rp == NULL) to align with
 *    CK-Ring's lack of per-consumer backpressure. Safe here because
 *    the benchmark fully drains between cycles.
 *  - CAPACITY (4096) and ITEM_SIZE (uint64_t) are fixed; changing them
 *    invalidates the comparison axis with the paper.
 *
 * Not allowed:
 *  - Add SCQ here — SCQ's enqueue spins when full and is incompatible
 *    with this binary's flag-based stop (use bench_scq.c instead).
 *  - Mix gated and ungated mseq paths in a single grid row.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "mpmc_seq.h"
#include <ck_ring.h>
/* SCQ (vendor/bench/scq) available but not integrated — lfring_enqueue
 * spins forever when full, incompatible with flag-based stop. */

#define CAPACITY 4096
#define ITEM_SIZE sizeof(uint64_t)
#define BENCH_SEC 2

typedef struct { void *ctx; _Atomic bool *run; _Atomic int *counting; _Atomic uint64_t *cnt; int id; int range; } targ_t;

static void *mseq_prod(void *a) {
    targ_t *t = a; mseq_t *q = t->ctx; uint64_t v = 0, n = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(q, t->id, NULL);
        if (s) { *(uint64_t *)s = v++; mseq_submit(q, t->id); if (atomic_load(t->counting)) n++; }
    }
    atomic_fetch_add(t->cnt, n); return NULL;
}
static void *mseq_cons(void *a) {
    targ_t *t = a; mseq_t *q = t->ctx; uint64_t n = 0;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim(q, NULL, 0, t->range);
        if (c.data) { if (atomic_load(t->counting)) n += c.count; mseq_release(q, NULL, 0, &c); }
    }
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

/* --- SCQ (Nikolaev, DISC 2019) --- */
typedef struct { struct lfring *ring; size_t order; } scqctx_t;
/* SCQ adapter removed — see vendor/bench/scq/ for the code.
 * Integration requires porting SCQ's own benchmark harness. */

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
        uint32_t parts = np; if(parts&(parts-1)){uint32_t v=parts;v--;v|=v>>1;v|=v>>2;v|=v>>4;v|=v>>8;v|=v>>16;parts=v+1;}

        printf("| **%dP/%dC** |", np, ncons);
        fprintf(stderr, "  %dP/%dC: ", np, ncons);

        for (int r = 0; r < nr; r++) {
            size_t sz = mseq_buffer_size(parts, CAPACITY, ITEM_SIZE);
            void *buf = aligned_alloc(MSEQ_CACHE_LINE, sz);
            mseq_t q; mseq_init(&q, buf, sz, parts, CAPACITY, ITEM_SIZE);
            double m = run(mseq_prod, mseq_cons, &q, np, ncons, ranges[r]);
            free(buf);
            printf(" %.1f |", m);
            fprintf(stderr, "R%d=%.1f ", ranges[r], m);
        }

        /* CK-Ring */
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
