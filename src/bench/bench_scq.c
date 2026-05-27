/*
 * bench_scq.c — head-to-head: mseq vs SCQD (Nikolaev SCQ with Data,
 *               DISC 2019). SCQD uses two SCQ rings: allocation queue
 *               (fq) + data queue (aq). Enqueue pops an index from fq,
 *               writes the value, pushes the index onto aq. Dequeue
 *               does the inverse.
 *
 * The SCQD pattern is the only one usable with our flag-based stop:
 * raw SCQ enqueue spins forever when the data ring is full, whereas
 * SCQD blocks only when every slot is in flight (true full).
 *
 * Invariants:
 *  - The free-list ring (fq) and the data ring (aq) MUST share the same
 *    capacity, otherwise indices leak and the queue stalls.
 *  - mseq is compared in gated mode at equal total capacity.
 *
 * Not allowed:
 *  - Replace SCQD with raw SCQ here — enqueue's full-spin blocks the
 *    bench from stopping cleanly.
 *  - Change the item width: SCQD stores uint64_t; widening requires
 *    re-tuning the comparison capacity.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>

#include "lf/lf.h"
#include "lfring_cas1.h"

/* Also benchmark our mseq for direct comparison */
#include "mpmc_seq.h"

#define BENCH_SEC 2

/* SCQD: two rings + value array */
typedef struct {
    struct lfring *aq;    /* allocation (data) queue */
    struct lfring *fq;    /* free index queue */
    uint64_t *val;        /* value storage indexed by ring index */
    size_t order;
    size_t half;
} scqd_t;

static int scqd_init(scqd_t *s, size_t order) {
    s->order = order;
    s->half = (size_t)1 << order;
    size_t ring_sz = LFRING_SIZE(order);

    s->aq = aligned_alloc(LFRING_ALIGN, ring_sz);
    s->fq = aligned_alloc(LFRING_ALIGN, ring_sz);
    s->val = calloc(s->half, sizeof(uint64_t));
    if (!s->aq || !s->fq || !s->val) return -1;

    lfring_init_empty(s->aq, order);    /* data queue starts empty */
    lfring_init_full(s->fq, order);     /* free queue starts full (all indices available) */
    return 0;
}

static void scqd_destroy(scqd_t *s) {
    free(s->aq); free(s->fq); free(s->val);
}

static inline int scqd_enqueue(scqd_t *s, uint64_t val) {
    size_t idx = lfring_dequeue(s->fq, s->order, false);  /* get free slot */
    if (idx == LFRING_EMPTY) return -1;  /* queue full */
    s->val[idx] = val;
    lfring_enqueue(s->aq, s->order, idx, false);  /* publish */
    return 0;
}

static inline int scqd_dequeue(scqd_t *s, uint64_t *val) {
    size_t idx = lfring_dequeue(s->aq, s->order, false);  /* get data slot */
    if (idx == LFRING_EMPTY) return -1;  /* queue empty */
    *val = s->val[idx];
    lfring_enqueue(s->fq, s->order, idx, true);  /* return to free pool */
    return 0;
}

/* ========================================================================
 * Benchmark harness
 * ======================================================================== */

typedef struct {
    void *ctx;
    _Atomic bool *run;
    _Atomic int *counting;
    _Atomic uint64_t *cnt;
    int id;
    int range;
} targ_t;

/* SCQD threads */
static void *scqd_prod(void *a) {
    targ_t *t = a; scqd_t *s = t->ctx;
    uint64_t v = 0, n = 0;
    while (atomic_load(t->run)) {
        if (scqd_enqueue(s, v++) == 0) { if (atomic_load(t->counting)) n++; }
    }
    atomic_fetch_add(t->cnt, n); return NULL;
}
static void *scqd_cons(void *a) {
    targ_t *t = a; scqd_t *s = t->ctx;
    uint64_t v, n = 0;
    while (atomic_load(t->run)) {
        if (scqd_dequeue(s, &v) == 0) { if (atomic_load(t->counting)) n++; }
    }
    atomic_fetch_add(t->cnt, n); return NULL;
}

/* mseq threads — UNGATED (rpool=NULL): these numbers are an upper bound
 * ceiling and are unsafe under wrap with slow consumers. For correct
 * gated numbers, use bench_fair.c which passes the rpool. */
static void *mseq_prod_fn(void *a) {
    targ_t *t = a; mseq_t *q = t->ctx;
    uint64_t v = 0, n = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(q, t->id, NULL);  /* ungated: rpool=NULL */
        if (s) { *(uint64_t *)s = v++; mseq_submit(q, t->id); if (atomic_load(t->counting)) n++; }
    }
    atomic_fetch_add(t->cnt, n); return NULL;
}
static void *mseq_cons_fn(void *a) {
    targ_t *t = a; mseq_t *q = t->ctx;
    uint64_t n = 0;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim(q, NULL, 0, t->range);  /* ungated: rpool=NULL */
        if (c.data) { if (atomic_load(t->counting)) n += c.count; mseq_release(q, NULL, 0, &c); }
    }
    atomic_fetch_add(t->cnt, n); return NULL;
}

typedef void *(*fn_t)(void *);

static double run_bench(fn_t pfn, fn_t cfn, void *ctx, int np, int nc, int range) {
    _Atomic bool run = true; _Atomic int counting = 0; _Atomic uint64_t cnt = 0;
    int tot = np + nc;
    pthread_t *th = malloc(sizeof(pthread_t) * tot);
    targ_t *ar = malloc(sizeof(targ_t) * tot);

    for (int i = 0; i < np; i++) {
        ar[i] = (targ_t){ctx, &run, &counting, &cnt, i, range};
        pthread_create(&th[i], NULL, pfn, &ar[i]);
    }
    for (int i = 0; i < nc; i++) {
        ar[np+i] = (targ_t){ctx, &run, &counting, &cnt, i, range};
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

    double elapsed = (t_end.tv_sec - t_start.tv_sec) + (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    double mops = atomic_load(&cnt) / elapsed / 1e6;
    free(th); free(ar);
    return mops;
}

int main(void) {
    int cfgs[][2] = {
        {1,1}, {2,2}, {4,4}, {8,8}, {16,16}, {4,1}, {4,8},
    };
    int ncfg = sizeof(cfgs)/sizeof(cfgs[0]);
    size_t order = 12;  /* 2^12 = 4096 slots */

    printf("# mseq (UNGATED) vs SCQ (SCQD with data) — Capacity=%zu\n", (size_t)1 << order);
    printf("# NOTE: mseq numbers are ungated (rpool=NULL) — upper bound, unsafe under wrap.\n");
    printf("# For correct gated numbers, use bench_fair.\n");
    printf("# AGGREGATE (prod+cons) throughput. Not comparable to bench_fair (consumer-only).\n\n");
    printf("| Config | mseq R=1 | mseq R=16 | mseq R=32 | SCQD |\n");
    printf("|--------|----------|-----------|-----------|------|\n");

    for (int c = 0; c < ncfg; c++) {
        int np = cfgs[c][0], nc = cfgs[c][1];
        uint32_t parts = np;
        if (parts & (parts-1)) { uint32_t v=parts; v--;v|=v>>1;v|=v>>2;v|=v>>4;v|=v>>8;v|=v>>16;parts=v+1; }

        fprintf(stderr, "  %dP/%dC: ", np, nc);

        /* mseq */
        double m1, m16, m32;
        {
            size_t sz = mseq_buffer_size(parts, 1 << order, sizeof(uint64_t));
            void *buf = aligned_alloc(MSEQ_CACHE_LINE, sz);
            mseq_t q;

            mseq_init(&q, buf, sz, parts, 1 << order, sizeof(uint64_t));
            m1 = run_bench(mseq_prod_fn, mseq_cons_fn, &q, np, nc, 1);
            free(buf);

            buf = aligned_alloc(MSEQ_CACHE_LINE, sz);
            mseq_init(&q, buf, sz, parts, 1 << order, sizeof(uint64_t));
            m16 = run_bench(mseq_prod_fn, mseq_cons_fn, &q, np, nc, 16);
            free(buf);

            buf = aligned_alloc(MSEQ_CACHE_LINE, sz);
            mseq_init(&q, buf, sz, parts, 1 << order, sizeof(uint64_t));
            m32 = run_bench(mseq_prod_fn, mseq_cons_fn, &q, np, nc, 32);
            free(buf);
        }

        /* SCQD (single ring, like Nikolaev's benchmark) */
        double scqd;
        {
            scqd_t s;
            scqd_init(&s, order);
            scqd = run_bench(scqd_prod, scqd_cons, &s, np, nc, 0);
            scqd_destroy(&s);
        }

        fprintf(stderr, "m1=%.1f m16=%.1f m32=%.1f scqd=%.1f\n", m1, m16, m32, scqd);
        printf("| **%dP/%dC** | %.1f | %.1f | %.1f | %.1f |\n",
               np, nc, m1, m16, m32, scqd);
    }

    printf("\n## Notes\n\n");
    printf("- **SCQD**: Nikolaev's SCQ with data (2 rings: free pool + data queue)\n");
    printf("- **mseq**: Our sequence-only design (partitioned, range claim)\n");
    printf("- SCQD is a single ring (not partitioned). All P producers share 1 ring.\n");
    printf("- mseq uses P partitions (1 per producer). Different design point.\n");

    return 0;
}
