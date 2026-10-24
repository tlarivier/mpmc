/*
 * main.c — mseq test suite (utest).
 *
 * Single-threaded tests cover the API, wrap-around, range claims,
 * sequences crossing UINT32_MAX, size overflow and the producer cache.
 * Threaded tests run gated. Producers write TAG(partition, position), and
 * consumers check each item against the positions they claimed, once after
 * the claim and once before the release. A passing run does not prove the
 * absence of races. The TSan build (make tsan) is the other check.
 *
 * Invariants:
 *  - Queue buffers come from ALIGNED_BUF (aligned, sized with
 *    mseq_buffer_size).
 *  - Each producer thread owns one partition and counts its submits, so
 *    the position it writes in TAG matches produce_seq.
 *  - Spin loops use a *_run flag and sched_yield.
 *  - wrap_extreme_u32 is the only test that writes produce_seq /
 *    consume_seq directly.
 *
 * Not allowed:
 *  - A threaded test with rp == NULL.
 *  - Two threads sharing a cid.
 *  - A threaded test that reads claims without checking their payload.
 */

#include "utest.h"
#include "mpmc_seq.h"
#include <pthread.h>
#include <time.h>

#define ALIGNED_BUF(name, np, sl, ss) \
    size_t name##_sz = mseq_buffer_size(np, sl, ss); \
    uint8_t name[name##_sz] __attribute__((aligned(MSEQ_CACHE_LINE)))

#define TAG(part, pos) (((uint64_t)(part) << 48) | (uint64_t)(pos))

static uint32_t claim_mismatches(const mseq_t *q, const mseq_claim_t *c) {
    uint32_t bad = 0;
    for (uint32_t i = 0; i < c->count; i++) {
        const uint64_t *item = mseq_claim_item(q, c, i);
        if (*item != TAG(c->part, c->start + i)) bad++;
    }
    return bad;
}

/* ============================================================================
 * Basic
 * ============================================================================ */

UTEST(mseq, init) {
    ALIGNED_BUF(buf, 4, 64, 64);
    mseq_t q;
    ASSERT_EQ(0, mseq_init(&q, buf, sizeof(buf), 4, 64, 64));
    ASSERT_EQ(4U, q.num_parts);
}

UTEST(mseq, init_invalid) {
    ALIGNED_BUF(buf, 4, 64, 64);
    mseq_t q;
    ASSERT_NE(0, mseq_init(&q, buf, sizeof(buf),  3, 64, 64));
    ASSERT_NE(0, mseq_init(&q, buf, sizeof(buf),  4,  7, 64));
    ASSERT_NE(0, mseq_init(&q, buf, sizeof(buf),  4, 64, 0));
    ASSERT_NE(0, mseq_init(&q, buf,            1, 4, 64, 64));
    ASSERT_NE(0, mseq_init(&q, NULL, sizeof(buf), 4, 64, 64));
}

UTEST(mseq, reserve_submit_claim_release) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    uint64_t *slot = mseq_reserve(&q, 0, NULL);
    ASSERT_TRUE(slot != NULL);
    *slot = 42;
    mseq_submit(&q, 0);

    mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
    ASSERT_TRUE(c.data != NULL);
    ASSERT_EQ(42ULL, *(uint64_t *)c.data);
    mseq_release(&q, NULL, 0, &c);
}

UTEST(mseq, claim_empty) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));
    mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
    ASSERT_TRUE(c.data == NULL);
}

UTEST(mseq, fill_and_drain) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    for (int i = 0; i < 16; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = i;
        mseq_submit(&q, 0);
    }
    ASSERT_TRUE(mseq_reserve(&q, 0, NULL) == NULL);

    for (int i = 0; i < 16; i++) {
        mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        ASSERT_EQ((uint64_t)i, *(uint64_t *)c.data);
        mseq_release(&q, NULL, 0, &c);
    }
    mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
    ASSERT_TRUE(c.data == NULL);
}

UTEST(mseq, range_claim) {
    ALIGNED_BUF(buf, 1, 32, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 32, sizeof(uint64_t));

    for (int i = 0; i < 8; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        *s = i;
        mseq_submit(&q, 0);
    }

    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    ASSERT_EQ(0ULL, *(uint64_t *)c.data);
    mseq_release(&q, NULL, 0, &c);

    c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    mseq_release(&q, NULL, 0, &c);
}

UTEST(mseq, wrap_correctness) {
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    for (int i = 0; i < 100; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = i;
        mseq_submit(&q, 0);

        mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        ASSERT_EQ((uint64_t)i, *(uint64_t *)c.data);
        mseq_release(&q, NULL, 0, &c);
    }
}

UTEST(mseq, multi_partition) {
    ALIGNED_BUF(buf, 4, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 4, 16, sizeof(uint64_t));

    for (uint32_t p = 0; p < 4; p++) {
        uint64_t *s = mseq_reserve(&q, p, NULL);
        *s = p * 100;
        mseq_submit(&q, p);
    }

    int found[4] = {0};
    for (int i = 0; i < 4; i++) {
        mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        uint64_t v = *(uint64_t *)c.data;
        found[v / 100] = 1;
        mseq_release(&q, NULL, 0, &c);
    }
    for (int i = 0; i < 4; i++) ASSERT_EQ(1, found[i]);
}

UTEST(mseq, buffer_size) {
    size_t sz = mseq_buffer_size(4, 256, 64);
    ASSERT_TRUE(sz > 0);
    ASSERT_EQ(0ULL, sz % MSEQ_CACHE_LINE);
}

/* ============================================================================
 * Concurrent: SPSC
 * ============================================================================ */

#define SPSC_N 50000

static mseq_t *g_spsc_q;
static mseq_rpool_t *g_spsc_rp;
static _Atomic int g_spsc_bad;

static void *spsc_prod(void *arg) {
    (void)arg;
    for (int i = 0; i < SPSC_N; i++) {
        uint64_t *s;
        while ((s = mseq_reserve(g_spsc_q, 0, g_spsc_rp)) == NULL) sched_yield();
        *s = i;
        mseq_submit(g_spsc_q, 0);
    }
    return NULL;
}

static void *spsc_cons(void *arg) {
    (void)arg; int n = 0;
    while (n < SPSC_N) {
        mseq_claim_t c = mseq_claim(g_spsc_q, g_spsc_rp, 0, 1);
        if (!c.data) { sched_yield(); continue; }
        if (*(uint64_t *)c.data != (uint64_t)n) atomic_fetch_add(&g_spsc_bad, 1);
        n++;
        mseq_release(g_spsc_q, g_spsc_rp, 0, &c);
    }
    return NULL;
}

UTEST(mseq, spsc) {
    ALIGNED_BUF(buf, 1, 256, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 256, sizeof(uint64_t));
    g_spsc_q = &q;

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);
    g_spsc_rp = &rp;
    atomic_store(&g_spsc_bad, 0);

    pthread_t p, c;
    pthread_create(&p, NULL, spsc_prod, NULL);
    pthread_create(&c, NULL, spsc_cons, NULL);
    pthread_join(p, NULL);
    pthread_join(c, NULL);

    ASSERT_EQ(0, atomic_load(&g_spsc_bad));
}

/* ============================================================================
 * Concurrent: MPMC no duplication, no loss
 * ============================================================================ */

#define DD_P 4
#define DD_N 10000
#define DD_TOTAL (DD_P * DD_N)

static mseq_t *g_dd_q;
static mseq_rpool_t *g_dd_rp;
static _Atomic uint8_t g_dd_seen[DD_TOTAL];
static _Atomic int g_dd_dup;
static _Atomic int g_dd_consumed;
static _Atomic int g_dd_stop;

static void *dd_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    for (int i = 0; i < DD_N; i++) {
        uint64_t *s;
        while ((s = mseq_reserve(g_dd_q, id, g_dd_rp)) == NULL) sched_yield();
        *s = (uint64_t)id * DD_N + i;
        mseq_submit(g_dd_q, id);
    }
    return NULL;
}

static void *dd_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    while (!atomic_load(&g_dd_stop) || atomic_load(&g_dd_consumed) < DD_TOTAL) {
        mseq_claim_t c = mseq_claim(g_dd_q, g_dd_rp, cid, 1);
        if (!c.data) { sched_yield(); continue; }
        uint64_t id = *(uint64_t *)c.data;
        mseq_release(g_dd_q, g_dd_rp, cid, &c);
        if (id < DD_TOTAL) {
            uint8_t prev = atomic_exchange(&g_dd_seen[id], 1);
            if (prev) atomic_fetch_add(&g_dd_dup, 1);
        }
        atomic_fetch_add(&g_dd_consumed, 1);
    }
    return NULL;
}

UTEST(mseq, no_dup_no_loss) {
    ALIGNED_BUF(buf, DD_P, 256, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), DD_P, 256, sizeof(uint64_t));
    g_dd_q = &q;

    size_t rp_sz = mseq_rpool_size(2);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 2);
    g_dd_rp = &rp;

    memset((void *)g_dd_seen, 0, sizeof(g_dd_seen));
    atomic_store(&g_dd_dup, 0);
    atomic_store(&g_dd_consumed, 0);
    atomic_store(&g_dd_stop, 0);

    pthread_t prods[DD_P], cons[2];
    for (int i = 0; i < DD_P; i++)
        pthread_create(&prods[i], NULL, dd_prod, (void *)(uintptr_t)i);
    for (int i = 0; i < 2; i++)
        pthread_create(&cons[i], NULL, dd_cons, (void *)(uintptr_t)i);

    for (int i = 0; i < DD_P; i++) pthread_join(prods[i], NULL);
    atomic_store(&g_dd_stop, 1);
    for (int i = 0; i < 2; i++) pthread_join(cons[i], NULL);

    ASSERT_EQ(0, atomic_load(&g_dd_dup));
    ASSERT_EQ(DD_TOTAL, atomic_load(&g_dd_consumed));
    int missing = 0;
    for (int i = 0; i < DD_TOTAL; i++)
        if (!atomic_load(&g_dd_seen[i])) missing++;
    ASSERT_EQ(0, missing);
}

/* ============================================================================
 * Concurrent: Stress 8P/4C with range
 * ============================================================================ */

static mseq_t *g_stress_q;
static mseq_rpool_t *g_stress_rp;
static _Atomic int g_stress_run;
static _Atomic uint64_t g_stress_ops;
static _Atomic uint64_t g_stress_bad;

static void *stress_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t v = 0;
    while (atomic_load(&g_stress_run)) {
        uint64_t *s = mseq_reserve(g_stress_q, id, g_stress_rp);
        if (s) { *s = TAG(id, v++); mseq_submit(g_stress_q, id); }
    }
    return NULL;
}

static void *stress_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_stress_run)) {
        mseq_claim_t c = mseq_claim(g_stress_q, g_stress_rp, cid, 16);
        if (!c.data) { sched_yield(); continue; }
        uint32_t bad = claim_mismatches(g_stress_q, &c);
        bad += claim_mismatches(g_stress_q, &c);
        if (bad) atomic_fetch_add(&g_stress_bad, bad);
        n += c.count;
        mseq_release(g_stress_q, g_stress_rp, cid, &c);
    }
    atomic_fetch_add(&g_stress_ops, n);
    return NULL;
}

UTEST(mseq, stress_8p4c) {
    ALIGNED_BUF(buf, 8, 256, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 8, 256, sizeof(uint64_t));
    g_stress_q = &q;

    size_t rp_sz = mseq_rpool_size(4);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 4);
    g_stress_rp = &rp;

    atomic_store(&g_stress_run, 1);
    atomic_store(&g_stress_ops, 0);
    atomic_store(&g_stress_bad, 0);

    pthread_t prods[8], cons[4];
    for (int i = 0; i < 8; i++)
        pthread_create(&prods[i], NULL, stress_prod, (void *)(uintptr_t)i);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, stress_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_stress_run, 0);

    for (int i = 0; i < 8; i++) pthread_join(prods[i], NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    uint64_t ops = atomic_load(&g_stress_ops);
    printf("\n  mseq stress (8P/4C R=16): %llu ops\n", (unsigned long long)ops);
    ASSERT_EQ(0ULL, (unsigned long long)atomic_load(&g_stress_bad));
    ASSERT_GT(ops, 50000ULL);
}

/* ============================================================================
 * Wrap-around with range claiming
 * ============================================================================ */

UTEST(mseq, wrap_range_claim) {
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    for (int i = 0; i < 6; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = i;
        mseq_submit(&q, 0);
    }
    for (int i = 0; i < 6; i++) {
        mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        ASSERT_EQ((uint64_t)i, *(uint64_t *)c.data);
        mseq_release(&q, NULL, 0, &c);
    }

    for (int i = 0; i < 6; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = 100 + i;
        mseq_submit(&q, 0);
    }

    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    ASSERT_EQ(6ULL, c.start);

    for (uint32_t i = 0; i < c.count; i++) {
        uint64_t *item = (uint64_t *)mseq_claim_item(&q, &c, i);
        ASSERT_TRUE(item != NULL);
        ASSERT_EQ(100ULL + i, *item);
    }

    mseq_release(&q, NULL, 0, &c);

    c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(2U, c.count);
    mseq_release(&q, NULL, 0, &c);
}

/* ============================================================================
 * Backpressure: slow consumer stalls producer
 * ============================================================================ */

UTEST(mseq, backpressure_gated) {
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);

    for (int i = 0; i < 8; i++) {
        uint64_t *s = mseq_reserve(&q, 0, &rp);
        ASSERT_TRUE(s != NULL);
        *s = i;
        mseq_submit(&q, 0);
    }

    ASSERT_TRUE(mseq_reserve(&q, 0, &rp) == NULL);

    mseq_claim_t c = mseq_claim(&q, &rp, 0, 1);
    ASSERT_TRUE(c.data != NULL);

    ASSERT_TRUE(mseq_reserve(&q, 0, &rp) == NULL);

    mseq_release(&q, &rp, 0, &c);

    for (int i = 0; i < 7; i++) {
        c = mseq_claim(&q, &rp, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        mseq_release(&q, &rp, 0, &c);
    }

    uint64_t *s = mseq_reserve(&q, 0, &rp);
    ASSERT_TRUE(s != NULL);
    *s = 99;
    mseq_submit(&q, 0);

    c = mseq_claim(&q, &rp, 0, 1);
    ASSERT_TRUE(c.data != NULL);
    ASSERT_EQ(99ULL, *(uint64_t *)c.data);
    mseq_release(&q, &rp, 0, &c);
}

/* ============================================================================
 * Extreme wrap: sequences cross UINT32_MAX boundary
 * ============================================================================ */

UTEST(mseq, wrap_extreme_u32) {
    ALIGNED_BUF(buf, 1, 4, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 4, sizeof(uint64_t));

    uint64_t seed = (uint64_t)UINT32_MAX - 2;
    atomic_store_explicit(&q.parts[0].produce_seq, seed, memory_order_relaxed);
    atomic_store_explicit(&q.parts[0].consume_seq, seed, memory_order_relaxed);

    for (int i = 0; i < 8; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = 0xDEAD0000ULL + i;
        mseq_submit(&q, 0);

        mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        ASSERT_EQ(0xDEAD0000ULL + i, *(uint64_t *)c.data);
        mseq_release(&q, NULL, 0, &c);
    }

    uint64_t final_seq = atomic_load_explicit(&q.parts[0].produce_seq, memory_order_relaxed);
    ASSERT_EQ(seed + 8, final_seq);
    ASSERT_TRUE(final_seq > (uint64_t)UINT32_MAX);

    atomic_store_explicit(&q.parts[0].produce_seq, (uint64_t)UINT32_MAX - 1, memory_order_relaxed);
    atomic_store_explicit(&q.parts[0].consume_seq, (uint64_t)UINT32_MAX - 1, memory_order_relaxed);

    for (int i = 0; i < 4; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = 500 + i;
        mseq_submit(&q, 0);
    }

    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    for (uint32_t i = 0; i < c.count; i++) {
        uint64_t *item = (uint64_t *)mseq_claim_item(&q, &c, i);
        ASSERT_TRUE(item != NULL);
        ASSERT_EQ(500ULL + i, *item);
    }
    mseq_release(&q, NULL, 0, &c);
}

/* ============================================================================
 * Stress: more consumers than partitions (C > P)
 * ============================================================================ */

static mseq_t *g_cp_q;
static mseq_rpool_t *g_cp_rp;
static _Atomic int g_cp_run;
static _Atomic uint64_t g_cp_produced;
static _Atomic uint64_t g_cp_consumed;
static _Atomic uint64_t g_cp_bad;

static void *cp_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_cp_run)) {
        uint64_t *s = mseq_reserve(g_cp_q, id, g_cp_rp);
        if (s) { *s = TAG(id, n++); mseq_submit(g_cp_q, id); }
    }
    atomic_fetch_add(&g_cp_produced, n);
    return NULL;
}

static void *cp_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_cp_run)) {
        mseq_claim_t c = mseq_claim(g_cp_q, g_cp_rp, cid, 4);
        if (!c.data) { sched_yield(); continue; }
        uint32_t bad = claim_mismatches(g_cp_q, &c);
        bad += claim_mismatches(g_cp_q, &c);
        if (bad) atomic_fetch_add(&g_cp_bad, bad);
        n += c.count;
        mseq_release(g_cp_q, g_cp_rp, cid, &c);
    }
    atomic_fetch_add(&g_cp_consumed, n);
    return NULL;
}

UTEST(mseq, stress_2p8c) {
    ALIGNED_BUF(buf, 2, 256, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 2, 256, sizeof(uint64_t));
    g_cp_q = &q;

    size_t rp_sz = mseq_rpool_size(8);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 8);
    g_cp_rp = &rp;

    atomic_store(&g_cp_run, 1);
    atomic_store(&g_cp_produced, 0);
    atomic_store(&g_cp_consumed, 0);
    atomic_store(&g_cp_bad, 0);

    pthread_t prods[2], cons[8];
    for (int i = 0; i < 2; i++)
        pthread_create(&prods[i], NULL, cp_prod, (void *)(uintptr_t)i);
    for (int i = 0; i < 8; i++)
        pthread_create(&cons[i], NULL, cp_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_cp_run, 0);

    for (int i = 0; i < 2; i++) pthread_join(prods[i], NULL);
    for (int i = 0; i < 8; i++) pthread_join(cons[i], NULL);

    uint64_t consumed = atomic_load(&g_cp_consumed);
    printf("\n  C>P stress (2P/8C R=4): produced=%llu consumed=%llu\n",
           (unsigned long long)atomic_load(&g_cp_produced),
           (unsigned long long)consumed);
    ASSERT_EQ(0ULL, (unsigned long long)atomic_load(&g_cp_bad));
    ASSERT_GT(consumed, 1000ULL);
}

/* ============================================================================
 * mask/start_idx in claim_t
 * ============================================================================ */

UTEST(mseq, claim_mask_start_idx) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    for (int i = 0; i < 8; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = (uint64_t)(i + 1) * 10;
        mseq_submit(&q, 0);
    }

    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);

    ASSERT_EQ(15U, c.mask);

    ASSERT_EQ(0ULL, c.start_idx);

    uint8_t *base = q.parts[c.part].data;
    for (uint32_t i = 0; i < c.count; i++) {
        uint32_t slot = (c.start_idx + i) & c.mask;
        uint64_t *item = (uint64_t *)&base[slot * q.slot_size];
        ASSERT_EQ((uint64_t)(i + 1) * 10, *item);
    }
    mseq_release(&q, NULL, 0, &c);

    c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    ASSERT_EQ(4ULL, c.start_idx);
    ASSERT_EQ(15U, c.mask);

    for (uint32_t i = 0; i < c.count; i++) {
        uint32_t slot = (c.start_idx + i) & c.mask;
        uint64_t *item = (uint64_t *)&base[slot * q.slot_size];
        ASSERT_EQ((uint64_t)(4 + i + 1) * 10, *item);
    }
    mseq_release(&q, NULL, 0, &c);
}

/* ============================================================================
 * Wrap-around with mask/start_idx specifically
 * ============================================================================ */

UTEST(mseq, wrap_range_with_mask) {
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    for (int i = 0; i < 6; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = i;
        mseq_submit(&q, 0);
    }
    for (int i = 0; i < 6; i++) {
        mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        mseq_release(&q, NULL, 0, &c);
    }

    for (int i = 0; i < 4; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = 500 + i;
        mseq_submit(&q, 0);
    }

    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    ASSERT_EQ(6ULL, c.start);

    ASSERT_EQ(6ULL, c.start_idx);
    ASSERT_EQ(7U, c.mask);

    uint8_t *base = q.parts[c.part].data;
    for (uint32_t i = 0; i < c.count; i++) {
        uint32_t slot = (c.start_idx + i) & c.mask;
        uint64_t *item = (uint64_t *)&base[slot * q.slot_size];
        ASSERT_EQ(500ULL + i, *item);
    }

    for (uint32_t i = 0; i < c.count; i++) {
        uint64_t *item = (uint64_t *)mseq_claim_item(&q, &c, i);
        ASSERT_TRUE(item != NULL);
        ASSERT_EQ(500ULL + i, *item);
    }

    mseq_release(&q, NULL, 0, &c);
}

/* ============================================================================
 * mseq_reader_t struct layout
 * ============================================================================ */

UTEST(mseq, reader_struct_size) {
    ASSERT_EQ((size_t)MSEQ_CACHE_LINE, sizeof(mseq_reader_t));

    ASSERT_EQ((size_t)0, sizeof(mseq_reader_t) % MSEQ_CACHE_LINE);

    ASSERT_EQ((size_t)(2 * MSEQ_CACHE_LINE), sizeof(mseq_part_t));

    size_t pool_sz = mseq_rpool_size(4);
    ASSERT_TRUE(pool_sz >= 4 * sizeof(mseq_reader_t));
    ASSERT_EQ((size_t)0, pool_sz % MSEQ_CACHE_LINE);
}

/* ============================================================================
 * Concurrent wrap stress with range > 1
 * ============================================================================ */

static mseq_t *g_cwr_q;
static mseq_rpool_t *g_cwr_rp;
static _Atomic int g_cwr_run;
static _Atomic uint64_t g_cwr_ops;
static _Atomic int g_cwr_corrupt;

static void *cwr_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t seq = 0;
    while (atomic_load(&g_cwr_run)) {
        uint64_t *s = mseq_reserve(g_cwr_q, id, g_cwr_rp);
        if (s) {
            *s = TAG(id, seq++);
            mseq_submit(g_cwr_q, id);
        }
    }
    return NULL;
}

static void *cwr_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_cwr_run)) {
        mseq_claim_t c = mseq_claim(g_cwr_q, g_cwr_rp, cid, 4);
        if (!c.data) { sched_yield(); continue; }

        uint8_t *base = g_cwr_q->parts[c.part].data;
        for (uint32_t i = 0; i < c.count; i++) {
            uint32_t slot = (c.start_idx + i) & c.mask;
            uint64_t *item = (uint64_t *)&base[slot * g_cwr_q->slot_size];
            if (*item != TAG(c.part, c.start + i))
                atomic_fetch_add(&g_cwr_corrupt, 1);
        }
        uint32_t late = claim_mismatches(g_cwr_q, &c);
        if (late) atomic_fetch_add(&g_cwr_corrupt, (int)late);
        n += c.count;
        mseq_release(g_cwr_q, g_cwr_rp, cid, &c);
    }
    atomic_fetch_add(&g_cwr_ops, n);
    return NULL;
}

UTEST(mseq, concurrent_wrap_range_stress) {
    ALIGNED_BUF(buf, 2, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 2, 16, sizeof(uint64_t));
    g_cwr_q = &q;

    size_t rp_sz = mseq_rpool_size(4);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 4);
    g_cwr_rp = &rp;

    atomic_store(&g_cwr_run, 1);
    atomic_store(&g_cwr_ops, 0);
    atomic_store(&g_cwr_corrupt, 0);

    pthread_t prods[2], cons[4];
    for (int i = 0; i < 2; i++)
        pthread_create(&prods[i], NULL, cwr_prod, (void *)(uintptr_t)i);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, cwr_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_cwr_run, 0);

    for (int i = 0; i < 2; i++) pthread_join(prods[i], NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    uint64_t ops = atomic_load(&g_cwr_ops);
    int corrupt = atomic_load(&g_cwr_corrupt);
    printf("\n  wrap range stress (2P/4C R=4): %llu ops, %d corrupt\n",
           (unsigned long long)ops, corrupt);
    ASSERT_EQ(0, corrupt);
    ASSERT_GT(ops, 1000ULL);
}

/* ============================================================================
 * Multiple simultaneous consumer releases
 * ============================================================================ */

static mseq_t *g_mcr_q;
static mseq_rpool_t *g_mcr_rp;
static _Atomic int g_mcr_run;
static _Atomic uint64_t g_mcr_bad;
static _Atomic uint64_t g_mcr_consumed[4];

static void *mcr_prod(void *arg) {
    (void)arg;
    uint64_t pos[4] = {0};
    while (atomic_load(&g_mcr_run)) {
        for (uint32_t p = 0; p < g_mcr_q->num_parts; p++) {
            uint64_t *s = mseq_reserve(g_mcr_q, p, g_mcr_rp);
            if (s) { *s = TAG(p, pos[p]++); mseq_submit(g_mcr_q, p); }
        }
    }
    return NULL;
}

static void *mcr_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_mcr_run)) {
        mseq_claim_t c = mseq_claim(g_mcr_q, g_mcr_rp, cid, 1);
        if (c.data) {
            uint32_t bad = claim_mismatches(g_mcr_q, &c);
            bad += claim_mismatches(g_mcr_q, &c);
            if (bad) atomic_fetch_add(&g_mcr_bad, bad);
            n++;
            mseq_release(g_mcr_q, g_mcr_rp, cid, &c);
        } else {
            sched_yield();
        }
    }
    atomic_store(&g_mcr_consumed[cid], n);
    return NULL;
}

UTEST(mseq, multi_consumer_release) {
    ALIGNED_BUF(buf, 4, 64, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 4, 64, sizeof(uint64_t));
    g_mcr_q = &q;

    size_t rp_sz = mseq_rpool_size(4);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 4);
    g_mcr_rp = &rp;

    atomic_store(&g_mcr_run, 1);
    for (int i = 0; i < 4; i++) atomic_store(&g_mcr_consumed[i], 0);
    atomic_store(&g_mcr_bad, 0);

    pthread_t prod, cons[4];
    pthread_create(&prod, NULL, mcr_prod, NULL);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, mcr_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 300000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_mcr_run, 0);

    pthread_join(prod, NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    uint64_t total = 0;
    for (int i = 0; i < 4; i++) {
        uint64_t n = atomic_load(&g_mcr_consumed[i]);
        total += n;
    }
    printf("\n  multi_consumer_release: total=%llu\n", (unsigned long long)total);
    ASSERT_EQ(0ULL, (unsigned long long)atomic_load(&g_mcr_bad));
    ASSERT_GT(total, 100ULL);
}

/* ============================================================================
 * Gated backpressure under concurrent load
 * ============================================================================ */

static mseq_t *g_bp_q;
static mseq_rpool_t *g_bp_rp;
static _Atomic int g_bp_run;
static _Atomic uint64_t g_bp_produced;
static _Atomic uint64_t g_bp_consumed;
static _Atomic int g_bp_overwrite;

static void *bp_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_bp_run)) {
        uint64_t *s = mseq_reserve(g_bp_q, id, g_bp_rp);
        if (s) {
            *s = TAG(id, n);
            mseq_submit(g_bp_q, id);
            n++;
        } else {
            sched_yield();
        }
    }
    atomic_fetch_add(&g_bp_produced, n);
    return NULL;
}

static void *bp_slow_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_bp_run)) {
        mseq_claim_t c = mseq_claim(g_bp_q, g_bp_rp, cid, 1);
        if (c.data) {
            int bad = (int)claim_mismatches(g_bp_q, &c);
            n++;
            struct timespec ts = {0, 100000};
            nanosleep(&ts, NULL);
            bad += (int)claim_mismatches(g_bp_q, &c);
            if (bad) atomic_fetch_add(&g_bp_overwrite, bad);
            mseq_release(g_bp_q, g_bp_rp, cid, &c);
        } else {
            sched_yield();
        }
    }
    atomic_store(&g_bp_consumed, n);
    return NULL;
}

UTEST(mseq, backpressure_concurrent) {
    ALIGNED_BUF(buf, 2, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 2, 8, sizeof(uint64_t));
    g_bp_q = &q;

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);
    g_bp_rp = &rp;

    atomic_store(&g_bp_run, 1);
    atomic_store(&g_bp_produced, 0);
    atomic_store(&g_bp_consumed, 0);
    atomic_store(&g_bp_overwrite, 0);

    pthread_t prods[2], cons;
    for (int i = 0; i < 2; i++)
        pthread_create(&prods[i], NULL, bp_prod, (void *)(uintptr_t)i);
    pthread_create(&cons, NULL, bp_slow_cons, (void *)(uintptr_t)0);

    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_bp_run, 0);

    for (int i = 0; i < 2; i++) pthread_join(prods[i], NULL);
    pthread_join(cons, NULL);

    uint64_t produced = atomic_load(&g_bp_produced);
    uint64_t consumed = atomic_load(&g_bp_consumed);
    int overwrite = atomic_load(&g_bp_overwrite);
    printf("\n  backpressure: produced=%llu consumed=%llu overwrite=%d\n",
           (unsigned long long)produced, (unsigned long long)consumed, overwrite);

    ASSERT_EQ(0, overwrite);
    ASSERT_GT(consumed, 0ULL);
    ASSERT_GT(produced, 0ULL);
}

/* ============================================================================
 * mseq_submit without mseq_reserve
 * ============================================================================ */

UTEST(mseq, submit_without_reserve) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    uint64_t prod_before = atomic_load(&q.parts[0].produce_seq);
    mseq_submit(&q, 0);
    uint64_t prod_after = atomic_load(&q.parts[0].produce_seq);

    ASSERT_EQ(prod_before, prod_after);

    mseq_submit(&q, 999);

    uint64_t *s = mseq_reserve(&q, 0, NULL);
    ASSERT_TRUE(s != NULL);
    *s = 42;
    mseq_submit(&q, 0);

    mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
    ASSERT_TRUE(c.data != NULL);
    ASSERT_EQ(42ULL, *(uint64_t *)c.data);
    mseq_release(&q, NULL, 0, &c);
}

/* ============================================================================
 * MSEQ_CACHE_LINE override via ifndef
 * ============================================================================ */

UTEST(mseq, cache_line_config) {
    ASSERT_TRUE(MSEQ_CACHE_LINE == 64 || MSEQ_CACHE_LINE == 128);

    ASSERT_EQ((size_t)(2 * MSEQ_CACHE_LINE), sizeof(mseq_part_t));

    ASSERT_EQ((size_t)MSEQ_CACHE_LINE, sizeof(mseq_reader_t));

    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));
    ASSERT_EQ((size_t)0, (uintptr_t)&q.parts[0] % MSEQ_CACHE_LINE);
}

/* ============================================================================
 * Multi-consumer active
 * ============================================================================ */

static mseq_t *g_mca_q;
static mseq_rpool_t *g_mca_rp;
static _Atomic int g_mca_run;
static _Atomic uint64_t g_mca_bad;
static _Atomic uint64_t g_mca_counter[4];

static void *mca_prod(void *arg) {
    (void)arg;
    uint64_t pos[4] = {0};
    while (atomic_load(&g_mca_run)) {
        for (uint32_t p = 0; p < g_mca_q->num_parts; p++) {
            uint64_t *s = mseq_reserve(g_mca_q, p, g_mca_rp);
            if (s) { *s = TAG(p, pos[p]++); mseq_submit(g_mca_q, p); }
        }
    }
    return NULL;
}

static void *mca_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    while (atomic_load(&g_mca_run)) {
        mseq_claim_t c = mseq_claim(g_mca_q, g_mca_rp, cid, 1);
        if (c.data) {
            uint32_t bad = claim_mismatches(g_mca_q, &c);
            bad += claim_mismatches(g_mca_q, &c);
            if (bad) atomic_fetch_add(&g_mca_bad, bad);
            atomic_fetch_add(&g_mca_counter[cid], 1);
            mseq_release(g_mca_q, g_mca_rp, cid, &c);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mseq, multi_consumer_active) {
    ALIGNED_BUF(buf, 4, 64, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 4, 64, sizeof(uint64_t));
    g_mca_q = &q;

    size_t rp_sz = mseq_rpool_size(4);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 4);
    g_mca_rp = &rp;

    atomic_store(&g_mca_run, 1);
    for (int i = 0; i < 4; i++) atomic_store(&g_mca_counter[i], 0);
    atomic_store(&g_mca_bad, 0);

    pthread_t prod, cons[4];
    pthread_create(&prod, NULL, mca_prod, NULL);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, mca_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 100000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_mca_run, 0);

    pthread_join(prod, NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    for (int i = 0; i < 4; i++) {
        uint64_t cnt = atomic_load(&g_mca_counter[i]);
        printf("\n  consumer[%d] ops=%llu", i, (unsigned long long)cnt);
        ASSERT_GT(cnt, 0ULL);
    }
    printf("\n");
    ASSERT_EQ(0ULL, (unsigned long long)atomic_load(&g_mca_bad));
}

/* ============================================================================
 * tl_start with multiple queues
 * ============================================================================ */

UTEST(mseq, tl_start_multi_queue) {
    ALIGNED_BUF(buf1, 4, 16, sizeof(uint64_t));
    ALIGNED_BUF(buf2, 4, 16, sizeof(uint64_t));
    mseq_t q1, q2;
    mseq_init(&q1, buf1, sizeof(buf1), 4, 16, sizeof(uint64_t));
    mseq_init(&q2, buf2, sizeof(buf2), 4, 16, sizeof(uint64_t));

    for (uint32_t p = 0; p < 4; p++) {
        for (int i = 0; i < 4; i++) {
            uint64_t *s1 = mseq_reserve(&q1, p, NULL);
            ASSERT_TRUE(s1 != NULL);
            *s1 = 1000 + p * 100 + i;
            mseq_submit(&q1, p);

            uint64_t *s2 = mseq_reserve(&q2, p, NULL);
            ASSERT_TRUE(s2 != NULL);
            *s2 = 2000 + p * 100 + i;
            mseq_submit(&q2, p);
        }
    }

    int q1_count = 0, q2_count = 0;
    int q1_valid = 0, q2_valid = 0;

    for (int round = 0; round < 16; round++) {
        mseq_claim_t c1 = mseq_claim(&q1, NULL, 0, 1);
        if (c1.data) {
            uint64_t val = *(uint64_t *)c1.data;
            if (val >= 1000 && val < 2000) q1_valid++;
            q1_count++;
            mseq_release(&q1, NULL, 0, &c1);
        }

        mseq_claim_t c2 = mseq_claim(&q2, NULL, 0, 1);
        if (c2.data) {
            uint64_t val = *(uint64_t *)c2.data;
            if (val >= 2000 && val < 3000) q2_valid++;
            q2_count++;
            mseq_release(&q2, NULL, 0, &c2);
        }
    }

    ASSERT_EQ(q1_count, q1_valid);
    ASSERT_EQ(q2_count, q2_valid);
    ASSERT_EQ(16, q1_count);
    ASSERT_EQ(16, q2_count);
}

/* ============================================================================
 * Edge cases
 * ============================================================================ */

UTEST(mseq, claim_zero_range) {
    ALIGNED_BUF(buf, 1, 4, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 4, sizeof(uint64_t));

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);

    uint64_t *s = mseq_reserve(&q, 0, &rp);
    ASSERT_TRUE(s != NULL);
    *s = 7;
    mseq_submit(&q, 0);

    mseq_claim_t c = mseq_claim(&q, &rp, 0, 0);
    ASSERT_TRUE(c.data == NULL);
    ASSERT_EQ(0U, c.count);
    ASSERT_EQ(MSEQ_IDLE, atomic_load(&rp.readers[0].part));

    c = mseq_claim(&q, &rp, 0, 1);
    ASSERT_TRUE(c.data != NULL);
    ASSERT_EQ(7ULL, *(uint64_t *)c.data);
    mseq_release(&q, &rp, 0, &c);
}

UTEST(mseq, buffer_size_overflow) {
    ASSERT_EQ((size_t)0, mseq_buffer_size(UINT32_MAX, UINT32_MAX, UINT32_MAX));
    if (sizeof(size_t) == 4)
        ASSERT_EQ((size_t)0, mseq_buffer_size(1, 1u << 20, 1u << 13));

    ALIGNED_BUF(buf, 1, 4, sizeof(uint64_t));
    mseq_t q;
    ASSERT_NE(0, mseq_init(&q, buf, sizeof(buf),
                           1u << 31, 1u << 31, UINT32_MAX));
}

UTEST(mseq, producer_cache_refresh) {
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);

    uint64_t pos = 0;
    for (int round = 0; round < 5; round++) {
        for (int i = 0; i < 8; i++) {
            uint64_t *s = mseq_reserve(&q, 0, &rp);
            ASSERT_TRUE(s != NULL);
            *s = TAG(0, pos++);
            mseq_submit(&q, 0);
        }
        ASSERT_TRUE(mseq_reserve(&q, 0, &rp) == NULL);

        mseq_claim_t c = mseq_claim(&q, &rp, 0, 8);
        ASSERT_EQ(8U, c.count);
        ASSERT_EQ(0U, claim_mismatches(&q, &c));
        ASSERT_TRUE(mseq_reserve(&q, 0, &rp) == NULL);
        mseq_release(&q, &rp, 0, &c);
    }
}

/* ============================================================================
 * Announcement protocol tests (mseq_announce.h coverage)
 * ============================================================================ */

#include "mseq_announce.h"

UTEST(mseq, announce_basic) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);

    for (int i = 0; i < 4; i++) {
        uint64_t *s = mseq_reserve(&q, 0, &rp);
        ASSERT_TRUE(s != NULL);
        *s = (uint64_t)(100 + i);
        mseq_submit(&q, 0);
    }

    mseq_claim_t c = mseq_claim_announced(&q, &rp, 0, 4);
    ASSERT_TRUE(c.data != NULL);
    ASSERT_EQ(4U, c.count);

    for (uint32_t i = 0; i < c.count; i++) {
        uint64_t *item = (uint64_t *)mseq_claim_item(&q, &c, i);
        ASSERT_TRUE(item != NULL);
        ASSERT_EQ(100ULL + i, *item);
    }
    mseq_release(&q, &rp, 0, &c);

    c = mseq_claim_announced(&q, &rp, 0, 1);
    ASSERT_TRUE(c.data == NULL);
}

static mseq_t *g_ann_mc_q;
static mseq_rpool_t *g_ann_mc_rp;
static _Atomic int g_ann_mc_stop;
static _Atomic int g_ann_mc_consumed;
static _Atomic int g_ann_mc_dup;

#define ANN_MC_N 10000
static _Atomic uint8_t g_ann_mc_seen[ANN_MC_N];

static void *ann_mc_prod(void *arg) {
    (void)arg;
    for (int i = 0; i < ANN_MC_N; i++) {
        uint64_t *s;
        while ((s = mseq_reserve(g_ann_mc_q, 0, g_ann_mc_rp)) == NULL) sched_yield();
        *s = (uint64_t)i;
        mseq_submit(g_ann_mc_q, 0);
    }
    return NULL;
}

static void *ann_mc_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    while (!atomic_load(&g_ann_mc_stop) || atomic_load(&g_ann_mc_consumed) < ANN_MC_N) {
        mseq_claim_t c = mseq_claim_announced(g_ann_mc_q, g_ann_mc_rp, cid, 1);
        if (!c.data) { sched_yield(); continue; }
        uint64_t id = *(uint64_t *)c.data;
        mseq_release(g_ann_mc_q, g_ann_mc_rp, cid, &c);
        if (id < ANN_MC_N) {
            uint8_t prev = atomic_exchange(&g_ann_mc_seen[id], 1);
            if (prev) atomic_fetch_add(&g_ann_mc_dup, 1);
        }
        atomic_fetch_add(&g_ann_mc_consumed, 1);
    }
    return NULL;
}

UTEST(mseq, announce_multi_consumer) {
    ALIGNED_BUF(buf, 1, 256, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 256, sizeof(uint64_t));
    g_ann_mc_q = &q;

    size_t rp_sz = mseq_rpool_size(4);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 4);
    g_ann_mc_rp = &rp;

    memset((void *)g_ann_mc_seen, 0, sizeof(g_ann_mc_seen));
    atomic_store(&g_ann_mc_dup, 0);
    atomic_store(&g_ann_mc_consumed, 0);
    atomic_store(&g_ann_mc_stop, 0);

    pthread_t prod, cons[4];
    pthread_create(&prod, NULL, ann_mc_prod, NULL);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, ann_mc_cons, (void *)(uintptr_t)i);

    pthread_join(prod, NULL);
    atomic_store(&g_ann_mc_stop, 1);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    ASSERT_EQ(0, atomic_load(&g_ann_mc_dup));
    ASSERT_EQ(ANN_MC_N, atomic_load(&g_ann_mc_consumed));
    int missing = 0;
    for (int i = 0; i < ANN_MC_N; i++)
        if (!atomic_load(&g_ann_mc_seen[i])) missing++;
    ASSERT_EQ(0, missing);
}

static mseq_t *g_ann_stress_q;
static mseq_rpool_t *g_ann_stress_rp;
static _Atomic int g_ann_stress_run;
static _Atomic uint64_t g_ann_stress_ops;
static _Atomic uint64_t g_ann_stress_bad;

static void *ann_stress_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t v = 0;
    while (atomic_load(&g_ann_stress_run)) {
        uint64_t *s = mseq_reserve(g_ann_stress_q, id, g_ann_stress_rp);
        if (s) { *s = TAG(id, v++); mseq_submit(g_ann_stress_q, id); }
    }
    return NULL;
}

static void *ann_stress_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_ann_stress_run)) {
        mseq_claim_t c = mseq_claim_announced(g_ann_stress_q, g_ann_stress_rp, cid, 16);
        if (!c.data) { sched_yield(); continue; }
        uint32_t bad = claim_mismatches(g_ann_stress_q, &c);
        bad += claim_mismatches(g_ann_stress_q, &c);
        if (bad) atomic_fetch_add(&g_ann_stress_bad, bad);
        n += c.count;
        mseq_release(g_ann_stress_q, g_ann_stress_rp, cid, &c);
    }
    atomic_fetch_add(&g_ann_stress_ops, n);
    return NULL;
}

UTEST(mseq, announce_concurrent) {
    ALIGNED_BUF(buf, 2, 128, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 2, 128, sizeof(uint64_t));
    g_ann_stress_q = &q;

    size_t rp_sz = mseq_rpool_size(4);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 4);
    g_ann_stress_rp = &rp;

    atomic_store(&g_ann_stress_run, 1);
    atomic_store(&g_ann_stress_ops, 0);
    atomic_store(&g_ann_stress_bad, 0);

    pthread_t prods[2], cons[4];
    for (int i = 0; i < 2; i++)
        pthread_create(&prods[i], NULL, ann_stress_prod, (void *)(uintptr_t)i);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, ann_stress_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_ann_stress_run, 0);

    for (int i = 0; i < 2; i++) pthread_join(prods[i], NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    uint64_t ops = atomic_load(&g_ann_stress_ops);
    printf("\n  announce stress (2P/4C R=16): %llu ops\n", (unsigned long long)ops);
    ASSERT_EQ(0ULL, (unsigned long long)atomic_load(&g_ann_stress_bad));
    ASSERT_GT(ops, 1000ULL);
}

UTEST_MAIN()
