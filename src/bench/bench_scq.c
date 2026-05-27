/*
 * bench_scq.c — mseq vs SCQD (Nikolaev, DISC 2019), enqueues and dequeues
 *               counted together.
 *
 * SCQD uses two SCQ rings: a free list of indices (fq) and a data queue
 * (aq). Enqueue takes an index from fq, writes the value and pushes the
 * index onto aq. Dequeue does the reverse.
 *
 * Invariants:
 *  - fq and aq have the same capacity.
 *  - 4096 slots in total on both sides. mseq runs gated and its consumers
 *    read their items.
 *
 * Not allowed:
 *  - Raw SCQ instead of SCQD: its enqueue spins on a full ring and the run
 *    never stops.
 *  - Comparing these numbers with bench_fair (consumer-only metric).
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

#include "mpmc_seq.h"
#include "bench_common.h"

#define BENCH_SEC 2

typedef struct {
    struct lfring *aq;
    struct lfring *fq;
    uint64_t *val;
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

    lfring_init_empty(s->aq, order);
    lfring_init_full(s->fq, order);
    return 0;
}

static void scqd_destroy(scqd_t *s) {
    free(s->aq); free(s->fq); free(s->val);
}

static inline int scqd_enqueue(scqd_t *s, uint64_t val) {
    size_t idx = lfring_dequeue(s->fq, s->order, false);
    if (idx == LFRING_EMPTY) return -1;
    s->val[idx] = val;
    lfring_enqueue(s->aq, s->order, idx, false);
    return 0;
}

static inline int scqd_dequeue(scqd_t *s, uint64_t *val) {
    size_t idx = lfring_dequeue(s->aq, s->order, false);
    if (idx == LFRING_EMPTY) return -1;
    *val = s->val[idx];
    lfring_enqueue(s->fq, s->order, idx, true);
    return 0;
}

/* ============================================================================
 * Benchmark harness
 * ============================================================================ */

typedef struct {
    void *ctx;
    _Atomic bool *run;
    _Atomic int *counting;
    _Atomic uint64_t *cnt;
    int id;
    int range;
} targ_t;

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

static void *mseq_prod_fn(void *a) {
    targ_t *t = a; mseq_bench_t *b = t->ctx;
    uint64_t v = 0, n = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(&b->q, t->id, &b->rp);
        if (s) { *(uint64_t *)s = v++; mseq_submit(&b->q, t->id); if (atomic_load(t->counting)) n++; }
    }
    atomic_fetch_add(t->cnt, n); return NULL;
}
static void *mseq_cons_fn(void *a) {
    targ_t *t = a; mseq_bench_t *b = t->ctx;
    uint64_t n = 0, sum = 0;
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
    size_t order = 12;

    printf("# mseq (gated) vs SCQD — total capacity %zu\n", (size_t)1 << order);
    printf("# Enqueues + dequeues per second. Not comparable to bench_fair (consumer-only).\n\n");
    printf("| Config | mseq R=1 | mseq R=16 | mseq R=32 | SCQD |\n");
    printf("|--------|----------|-----------|-----------|------|\n");

    for (int c = 0; c < ncfg; c++) {
        int np = cfgs[c][0], nc = cfgs[c][1];
        uint32_t parts = mseq_bench_pow2(np);

        fprintf(stderr, "  %dP/%dC: ", np, nc);

        double m1, m16, m32;
        {
            int ranges[3] = {1, 16, 32};
            double *out[3] = {&m1, &m16, &m32};
            for (int r = 0; r < 3; r++) {
                mseq_bench_t b;
                if (mseq_bench_init(&b, parts, 1u << order, nc)) { fprintf(stderr, "init failed\n"); return 1; }
                *out[r] = run_bench(mseq_prod_fn, mseq_cons_fn, &b, np, nc, ranges[r]);
                mseq_bench_free(&b);
            }
        }

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
    printf("- **mseq**: partitioned, range claims, gated, 4096 slots split across partitions\n");
    printf("- SCQD is a single ring (not partitioned). All P producers share 1 ring.\n");
    printf("- mseq uses P partitions (1 per producer). Different design point.\n");

    return 0;
}
