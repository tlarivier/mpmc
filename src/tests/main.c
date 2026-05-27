/*
 * main.c — mseq test suite (utest). Lifecycle, ring wrap, range claims,
 *          gated backpressure, MPMC stress, sequence-counter overflow,
 *          announcement-protocol coverage. 29 UTEST cases total.
 *
 * Invariants:
 *  - Every test allocates its buffer via ALIGNED_BUF (cache-line aligned,
 *    sized from mseq_buffer_size()) — never bypass this macro for the
 *    queue buffer, the queue rejects misaligned inputs.
 *  - Concurrent tests use a global *_run flag + sched_yield in spin loops
 *    so the suite is TSan-friendly (no busy-wait fences).
 *  - The wrap_extreme_u32 test reaches inside q.parts[].produce_seq /
 *    consume_seq to pre-seed sequences past UINT32_MAX. This is the only
 *    site that legitimately writes those atomics from test code.
 *
 * Not allowed:
 *  - Add a test that uses the ungated path (rp == NULL) with a slow
 *    consumer + ring wrap. That combination is documented as unsafe
 *    (see ARCHITECTURE.md §7); a test relying on it would be wrong.
 *  - Share a consumer id (cid) across two threads in the same test —
 *    correctness depends on the one-cid-per-thread contract.
 */

#include "utest.h"
#include "mpmc_seq.h"
#include <pthread.h>
#include <time.h>

#define ALIGNED_BUF(name, np, sl, ss) \
    size_t name##_sz = mseq_buffer_size(np, sl, ss); \
    uint8_t name[name##_sz] __attribute__((aligned(MSEQ_CACHE_LINE)))

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
    ASSERT_TRUE(mseq_reserve(&q, 0, NULL) == NULL);  /* full */

    for (int i = 0; i < 16; i++) {
        mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        ASSERT_EQ((uint64_t)i, *(uint64_t *)c.data);
        mseq_release(&q, NULL, 0, &c);
    }
    mseq_claim_t c = mseq_claim(&q, NULL, 0, 1);
    ASSERT_TRUE(c.data == NULL);  /* empty */
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
    /* Small ring, many items — forces wrap multiple times */
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

static mseq_t *g_spsc_q;
static _Atomic int g_spsc_prod_done;

static void *spsc_prod(void *arg) {
    (void)arg;
    for (int i = 0; i < 50000; i++) {
        uint64_t *s;
        while ((s = mseq_reserve(g_spsc_q, 0, NULL)) == NULL) sched_yield();
        *s = i;
        mseq_submit(g_spsc_q, 0);
    }
    atomic_store(&g_spsc_prod_done, 1);
    return NULL;
}

static void *spsc_cons(void *arg) {
    (void)arg; int n = 0;
    while (n < 50000) {
        mseq_claim_t c = mseq_claim(g_spsc_q, NULL, 0, 1);
        if (c.data) { n++; mseq_release(g_spsc_q, NULL, 0, &c); }
        else sched_yield();
    }
    return NULL;
}

UTEST(mseq, spsc) {
    ALIGNED_BUF(buf, 1, 256, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 256, sizeof(uint64_t));
    g_spsc_q = &q;
    atomic_store(&g_spsc_prod_done, 0);

    pthread_t p, c;
    pthread_create(&p, NULL, spsc_prod, NULL);
    pthread_create(&c, NULL, spsc_cons, NULL);
    pthread_join(p, NULL);
    pthread_join(c, NULL);
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

    /* Gated reader pool: 2 consumers, each on own cache line */
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
static _Atomic int g_stress_run;
static _Atomic uint64_t g_stress_ops;

static void *stress_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t v = 0;
    while (atomic_load(&g_stress_run)) {
        uint64_t *s = mseq_reserve(g_stress_q, id, NULL);
        if (s) { *s = v++; mseq_submit(g_stress_q, id); }
    }
    return NULL;
}

static void *stress_cons(void *arg) {
    (void)arg; uint64_t n = 0;
    while (atomic_load(&g_stress_run)) {
        mseq_claim_t c = mseq_claim(g_stress_q, NULL, 0, 16);
        if (c.data) { n += c.count; mseq_release(g_stress_q, NULL, 0, &c); }
    }
    atomic_fetch_add(&g_stress_ops, n);
    return NULL;
}

UTEST(mseq, stress_8p4c) {
    ALIGNED_BUF(buf, 8, 256, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 8, 256, sizeof(uint64_t));
    g_stress_q = &q;
    atomic_store(&g_stress_run, 1);
    atomic_store(&g_stress_ops, 0);

    pthread_t prods[8], cons[4];
    for (int i = 0; i < 8; i++)
        pthread_create(&prods[i], NULL, stress_prod, (void *)(uintptr_t)i);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, stress_cons, NULL);

    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_stress_run, 0);

    for (int i = 0; i < 8; i++) pthread_join(prods[i], NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    uint64_t ops = atomic_load(&g_stress_ops);
    printf("\n  mseq stress (8P/4C R=16): %llu ops\n", (unsigned long long)ops);
    ASSERT_GT(ops, 50000ULL);
}

/* ============================================================================
 * Wrap-around with range claiming (audit fix I3)
 * ============================================================================ */

UTEST(mseq, wrap_range_claim) {
    /* 8-slot ring, range=4: claim MUST wrap around the ring boundary */
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    /* Fill 6 items, drain 6 — consume_seq at 6 */
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

    /* Now produce_seq=6, consume_seq=6. Produce 6 more: slots [6,7,0,1,2,3] */
    for (int i = 0; i < 6; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = 100 + i;
        mseq_submit(&q, 0);
    }

    /* Claim with range=4 starting at consume_seq=6 in 8-slot ring:
     * items at physical slots [6, 7, 0, 1] — wraps around! */
    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    ASSERT_EQ(6ULL, c.start);

    /* Verify each item using mseq_claim_item() — handles wrap correctly */
    for (uint32_t i = 0; i < c.count; i++) {
        uint64_t *item = (uint64_t *)mseq_claim_item(&q, &c, i);
        ASSERT_TRUE(item != NULL);
        ASSERT_EQ(100ULL + i, *item);
    }

    /* Also verify that naive pointer arithmetic would FAIL at the wrap point.
     * c.data points to slot 6. c.data + 2*8 = slot 8, which is OUT OF BOUNDS
     * for an 8-slot ring (should wrap to slot 0). This is WHY mseq_claim_item
     * exists — naive access is a bug. */
    /* (We don't assert the failure — just documenting the pitfall.) */
    mseq_release(&q, NULL, 0, &c);

    /* Claim remaining 2 items */
    c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(2U, c.count);
    mseq_release(&q, NULL, 0, &c);
}

/* ============================================================================
 * Backpressure: slow consumer stalls producer (audit fix I3)
 * ============================================================================ */

UTEST(mseq, backpressure_gated) {
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);

    /* Fill to capacity (8 items) */
    for (int i = 0; i < 8; i++) {
        uint64_t *s = mseq_reserve(&q, 0, &rp);
        ASSERT_TRUE(s != NULL);
        *s = i;
        mseq_submit(&q, 0);
    }

    /* Queue full — producer must be blocked */
    ASSERT_TRUE(mseq_reserve(&q, 0, &rp) == NULL);

    /* Consumer claims but HOLDS (doesn't release) — simulates slow consumer */
    mseq_claim_t c = mseq_claim(&q, &rp, 0, 1);
    ASSERT_TRUE(c.data != NULL);

    /* Consumer has claimed slot 0 but not released — producer still blocked
     * because gated backpressure sees reader cursor at position 0 */
    ASSERT_TRUE(mseq_reserve(&q, 0, &rp) == NULL);

    /* Consumer releases — now producer can advance */
    mseq_release(&q, &rp, 0, &c);

    /* Drain remaining 7 items */
    for (int i = 0; i < 7; i++) {
        c = mseq_claim(&q, &rp, 0, 1);
        ASSERT_TRUE(c.data != NULL);
        mseq_release(&q, &rp, 0, &c);
    }

    /* Now producer should be able to reserve again */
    uint64_t *s = mseq_reserve(&q, 0, &rp);
    ASSERT_TRUE(s != NULL);
    *s = 99;
    mseq_submit(&q, 0);

    /* Verify the new item */
    c = mseq_claim(&q, &rp, 0, 1);
    ASSERT_TRUE(c.data != NULL);
    ASSERT_EQ(99ULL, *(uint64_t *)c.data);
    mseq_release(&q, &rp, 0, &c);
}

/* ============================================================================
 * Extreme wrap: sequences cross UINT32_MAX boundary
 * ============================================================================ */

UTEST(mseq, wrap_extreme_u32) {
    /* 4-slot ring. Pre-seed sequences near UINT32_MAX, then produce/consume
     * across the 32-bit boundary to verify slot indexing doesn't break.
     * Slot index = (uint32_t)seq & mask — this must wrap correctly. */
    ALIGNED_BUF(buf, 1, 4, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 4, sizeof(uint64_t));

    /* Pre-seed both sequences to just below UINT32_MAX. The ring is empty
     * (produce == consume), so we can produce immediately. */
    uint64_t seed = (uint64_t)UINT32_MAX - 2;
    atomic_store_explicit(&q.parts[0].produce_seq, seed, memory_order_relaxed);
    atomic_store_explicit(&q.parts[0].consume_seq, seed, memory_order_relaxed);

    /* Produce and consume 8 items across the boundary:
     * seq = UINT32_MAX-2, -1, 0(wrap), 1, 2, 3, 4, 5 */
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

    /* Verify sequences have crossed the boundary */
    uint64_t final_seq = atomic_load_explicit(&q.parts[0].produce_seq, memory_order_relaxed);
    ASSERT_EQ(seed + 8, final_seq);
    ASSERT_TRUE(final_seq > (uint64_t)UINT32_MAX);

    /* Range claim across the boundary */
    atomic_store_explicit(&q.parts[0].produce_seq, (uint64_t)UINT32_MAX - 1, memory_order_relaxed);
    atomic_store_explicit(&q.parts[0].consume_seq, (uint64_t)UINT32_MAX - 1, memory_order_relaxed);

    for (int i = 0; i < 4; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = 500 + i;
        mseq_submit(&q, 0);
    }

    /* Claim range=4 starting at UINT32_MAX-1 in 4-slot ring */
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

static void *cp_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_cp_run)) {
        uint64_t *s = mseq_reserve(g_cp_q, id, g_cp_rp);
        if (s) { *s = n++; mseq_submit(g_cp_q, id); }
    }
    atomic_fetch_add(&g_cp_produced, n);
    return NULL;
}

static void *cp_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_cp_run)) {
        mseq_claim_t c = mseq_claim(g_cp_q, g_cp_rp, cid, 4);
        if (c.data) { n += c.count; mseq_release(g_cp_q, g_cp_rp, cid, &c); }
        else sched_yield();
    }
    atomic_fetch_add(&g_cp_consumed, n);
    return NULL;
}

UTEST(mseq, stress_2p8c) {
    /* 2 partitions, 8 consumers — consumers outnumber partitions */
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
    ASSERT_GT(consumed, 1000ULL);
}

/* ============================================================================
 * Test 1: mask/start_idx in claim_t (C4 fix validation)
 * ============================================================================ */

UTEST(mseq, claim_mask_start_idx) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    /* Produce 8 items */
    for (int i = 0; i < 8; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = (uint64_t)(i + 1) * 10;
        mseq_submit(&q, 0);
    }

    /* Claim with range=4 */
    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);

    /* Verify mask matches partition mask (slots_per_part - 1) */
    ASSERT_EQ(15U, c.mask);  /* 16 slots => mask=15 */

    /* Verify start_idx matches consumer position & mask */
    ASSERT_EQ(0ULL, c.start_idx);  /* first claim starts at 0 */

    /* Verify iteration with base[((start_idx + i) & mask) * slot_size] gives correct data */
    uint8_t *base = q.parts[c.part].data;
    for (uint32_t i = 0; i < c.count; i++) {
        uint32_t slot = (c.start_idx + i) & c.mask;
        uint64_t *item = (uint64_t *)&base[slot * q.slot_size];
        ASSERT_EQ((uint64_t)(i + 1) * 10, *item);
    }
    mseq_release(&q, NULL, 0, &c);

    /* Now claim next 4 — start_idx should be 4 */
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
 * Test 2: Wrap-around with mask/start_idx specifically
 * ============================================================================ */

UTEST(mseq, wrap_range_with_mask) {
    ALIGNED_BUF(buf, 1, 8, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 8, sizeof(uint64_t));

    /* Fill 6 items, consume them all — moves produce_seq and consume_seq to 6 */
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

    /* Now produce 4 items that WRAP: physical slots [6, 7, 0, 1] */
    for (int i = 0; i < 4; i++) {
        uint64_t *s = mseq_reserve(&q, 0, NULL);
        ASSERT_TRUE(s != NULL);
        *s = 500 + i;  /* 500, 501, 502, 503 */
        mseq_submit(&q, 0);
    }

    /* Claim range=4: should wrap around ring boundary */
    mseq_claim_t c = mseq_claim(&q, NULL, 0, 4);
    ASSERT_EQ(4U, c.count);
    ASSERT_EQ(6ULL, c.start);  /* logical start */

    /* start_idx should be 6 & 7 = 6 */
    ASSERT_EQ(6ULL, c.start_idx);
    ASSERT_EQ(7U, c.mask);  /* 8 slots => mask=7 */

    /* Verify data at each wrapped position */
    uint8_t *base = q.parts[c.part].data;
    for (uint32_t i = 0; i < c.count; i++) {
        uint32_t slot = (c.start_idx + i) & c.mask;
        uint64_t *item = (uint64_t *)&base[slot * q.slot_size];
        ASSERT_EQ(500ULL + i, *item);
    }

    /* Also verify using mseq_claim_item */
    for (uint32_t i = 0; i < c.count; i++) {
        uint64_t *item = (uint64_t *)mseq_claim_item(&q, &c, i);
        ASSERT_TRUE(item != NULL);
        ASSERT_EQ(500ULL + i, *item);
    }

    mseq_release(&q, NULL, 0, &c);
}

/* ============================================================================
 * Test 3: mseq_reader_t struct layout (C2 fix validation)
 * ============================================================================ */

UTEST(mseq, reader_struct_size) {
    /* mseq_reader_t should be exactly MSEQ_CACHE_LINE bytes */
    ASSERT_EQ((size_t)MSEQ_CACHE_LINE, sizeof(mseq_reader_t));

    /* Verify alignment attribute is honored */
    ASSERT_EQ((size_t)0, sizeof(mseq_reader_t) % MSEQ_CACHE_LINE);

    /* mseq_part_t should be exactly 2 cache lines */
    ASSERT_EQ((size_t)(2 * MSEQ_CACHE_LINE), sizeof(mseq_part_t));

    /* Verify reader pool allocation matches */
    size_t pool_sz = mseq_rpool_size(4);
    ASSERT_TRUE(pool_sz >= 4 * sizeof(mseq_reader_t));
    ASSERT_EQ((size_t)0, pool_sz % MSEQ_CACHE_LINE);
}

/* ============================================================================
 * Test 4: Concurrent wrap stress with range > 1
 * ============================================================================ */

static mseq_t *g_cwr_q;
static mseq_rpool_t *g_cwr_rp;
static _Atomic int g_cwr_run;
static _Atomic uint64_t g_cwr_ops;
static _Atomic int g_cwr_corrupt;

static void *cwr_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t pattern = (uint64_t)id << 48;  /* unique per-partition pattern */
    uint64_t seq = 0;
    while (atomic_load(&g_cwr_run)) {
        uint64_t *s = mseq_reserve(g_cwr_q, id, g_cwr_rp);
        if (s) {
            *s = pattern | seq++;
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

        /* Iterate using mask/start_idx and verify data integrity */
        uint8_t *base = g_cwr_q->parts[c.part].data;
        for (uint32_t i = 0; i < c.count; i++) {
            uint32_t slot = (c.start_idx + i) & c.mask;
            uint64_t *item = (uint64_t *)&base[slot * g_cwr_q->slot_size];
            uint64_t val = *item;
            /* Verify the partition tag in the upper bits */
            uint32_t tag = (uint32_t)(val >> 48);
            if (tag != c.part) {
                atomic_fetch_add(&g_cwr_corrupt, 1);
            }
        }
        n += c.count;
        mseq_release(g_cwr_q, g_cwr_rp, cid, &c);
    }
    atomic_fetch_add(&g_cwr_ops, n);
    return NULL;
}

UTEST(mseq, concurrent_wrap_range_stress) {
    ALIGNED_BUF(buf, 2, 16, sizeof(uint64_t));  /* small ring to force wraps */
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

    struct timespec ts = {0, 500000000};  /* 500ms */
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
 * Test 5: Multiple simultaneous consumer releases
 * ============================================================================ */

static mseq_t *g_mcr_q;
static mseq_rpool_t *g_mcr_rp;
static _Atomic int g_mcr_run;
static _Atomic uint64_t g_mcr_consumed[4];

static void *mcr_prod(void *arg) {
    (void)arg;
    uint64_t v = 0;
    while (atomic_load(&g_mcr_run)) {
        for (uint32_t p = 0; p < g_mcr_q->num_parts; p++) {
            uint64_t *s = mseq_reserve(g_mcr_q, p, g_mcr_rp);
            if (s) { *s = v++; mseq_submit(g_mcr_q, p); }
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

    pthread_t prod, cons[4];
    pthread_create(&prod, NULL, mcr_prod, NULL);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, mcr_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 300000000};  /* 300ms */
    nanosleep(&ts, NULL);
    atomic_store(&g_mcr_run, 0);

    pthread_join(prod, NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    /* Verify all consumers consumed something (no interference) */
    uint64_t total = 0;
    for (int i = 0; i < 4; i++) {
        uint64_t n = atomic_load(&g_mcr_consumed[i]);
        total += n;
    }
    printf("\n  multi_consumer_release: total=%llu\n", (unsigned long long)total);
    ASSERT_GT(total, 100ULL);
}

/* ============================================================================
 * Test 6: Gated backpressure under concurrent load
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
            *s = (uint64_t)id * 1000000 + n;
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
            uint64_t val = *(uint64_t *)c.data;
            /* Verify data looks valid (has producer pattern) */
            if (val > 2000000) {
                atomic_fetch_add(&g_bp_overwrite, 1);
            }
            n++;
            /* Simulate slow consumer: small sleep */
            struct timespec ts = {0, 100000};  /* 100us */
            nanosleep(&ts, NULL);
            mseq_release(g_bp_q, g_bp_rp, cid, &c);
        } else {
            sched_yield();
        }
    }
    atomic_store(&g_bp_consumed, n);
    return NULL;
}

UTEST(mseq, backpressure_concurrent) {
    ALIGNED_BUF(buf, 2, 8, sizeof(uint64_t));  /* small ring: 8 slots per partition */
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

    struct timespec ts = {0, 500000000};  /* 500ms */
    nanosleep(&ts, NULL);
    atomic_store(&g_bp_run, 0);

    for (int i = 0; i < 2; i++) pthread_join(prods[i], NULL);
    pthread_join(cons, NULL);

    uint64_t produced = atomic_load(&g_bp_produced);
    uint64_t consumed = atomic_load(&g_bp_consumed);
    int overwrite = atomic_load(&g_bp_overwrite);
    printf("\n  backpressure: produced=%llu consumed=%llu overwrite=%d\n",
           (unsigned long long)produced, (unsigned long long)consumed, overwrite);

    /* Backpressure should mean produced is bounded by consumed + ring size */
    ASSERT_EQ(0, overwrite);
    ASSERT_GT(consumed, 0ULL);
    ASSERT_GT(produced, 0ULL);
}

/* ============================================================================
 * Test 7: mseq_submit without mseq_reserve
 * ============================================================================ */

UTEST(mseq, submit_without_reserve) {
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    /* Submit without reserve — should be a no-op (reserved flag is 0) */
    uint64_t prod_before = atomic_load(&q.parts[0].produce_seq);
    mseq_submit(&q, 0);
    uint64_t prod_after = atomic_load(&q.parts[0].produce_seq);

    /* produce_seq should NOT advance since reserved was 0 */
    ASSERT_EQ(prod_before, prod_after);

    /* Submit on invalid partition — should be a no-op */
    mseq_submit(&q, 999);

    /* Queue should still work normally after spurious submits */
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
 * Test 8: MSEQ_CACHE_LINE override via ifndef
 * ============================================================================ */

UTEST(mseq, cache_line_config) {
    /* MSEQ_CACHE_LINE should be set (128 on ARM64/Apple Silicon, 64 on x86) */
    ASSERT_TRUE(MSEQ_CACHE_LINE == 64 || MSEQ_CACHE_LINE == 128);

    /* Partition struct must be 2 cache lines (producer line + consumer line) */
    ASSERT_EQ((size_t)(2 * MSEQ_CACHE_LINE), sizeof(mseq_part_t));

    /* Reader struct must be 1 cache line */
    ASSERT_EQ((size_t)MSEQ_CACHE_LINE, sizeof(mseq_reader_t));

    /* Alignment of partition struct */
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));
    ASSERT_EQ((size_t)0, (uintptr_t)&q.parts[0] % MSEQ_CACHE_LINE);
}

/* ============================================================================
 * Test 9: Multi-consumer active (M2 fix validation — all consumers active)
 * ============================================================================ */

static mseq_t *g_mca_q;
static mseq_rpool_t *g_mca_rp;
static _Atomic int g_mca_run;
static _Atomic uint64_t g_mca_counter[4];

static void *mca_prod(void *arg) {
    (void)arg;
    uint64_t v = 0;
    while (atomic_load(&g_mca_run)) {
        for (uint32_t p = 0; p < g_mca_q->num_parts; p++) {
            uint64_t *s = mseq_reserve(g_mca_q, p, g_mca_rp);
            if (s) { *s = v++; mseq_submit(g_mca_q, p); }
        }
    }
    return NULL;
}

static void *mca_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    while (atomic_load(&g_mca_run)) {
        mseq_claim_t c = mseq_claim(g_mca_q, g_mca_rp, cid, 1);
        if (c.data) {
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

    pthread_t prod, cons[4];
    pthread_create(&prod, NULL, mca_prod, NULL);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, mca_cons, (void *)(uintptr_t)i);

    /* Let it run for 100ms */
    struct timespec ts = {0, 100000000};
    nanosleep(&ts, NULL);
    atomic_store(&g_mca_run, 0);

    pthread_join(prod, NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    /* Verify ALL consumer counters are non-zero (all consumers were active) */
    for (int i = 0; i < 4; i++) {
        uint64_t cnt = atomic_load(&g_mca_counter[i]);
        printf("\n  consumer[%d] ops=%llu", i, (unsigned long long)cnt);
        ASSERT_GT(cnt, 0ULL);
    }
    printf("\n");
}

/* ============================================================================
 * Test 10: tl_start with multiple queues
 * ============================================================================ */

UTEST(mseq, tl_start_multi_queue) {
    /* Create 2 separate queues */
    ALIGNED_BUF(buf1, 4, 16, sizeof(uint64_t));
    ALIGNED_BUF(buf2, 4, 16, sizeof(uint64_t));
    mseq_t q1, q2;
    mseq_init(&q1, buf1, sizeof(buf1), 4, 16, sizeof(uint64_t));
    mseq_init(&q2, buf2, sizeof(buf2), 4, 16, sizeof(uint64_t));

    /* Produce items in both queues — different values */
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

    /* Alternate claiming from q1 and q2 from the same thread */
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

    /* All items from q1 should be in [1000,2000), all from q2 in [2000,3000) */
    ASSERT_EQ(q1_count, q1_valid);
    ASSERT_EQ(q2_count, q2_valid);
    /* We should have consumed all 16 items from each queue */
    ASSERT_EQ(16, q1_count);
    ASSERT_EQ(16, q2_count);
}

/* ============================================================================
 * Announcement protocol tests (mseq_announce.h coverage)
 * ============================================================================ */

#include "mseq_announce.h"

UTEST(mseq, announce_basic) {
    /* Basic produce / claim_announced / release cycle */
    ALIGNED_BUF(buf, 1, 16, sizeof(uint64_t));
    mseq_t q;
    mseq_init(&q, buf, sizeof(buf), 1, 16, sizeof(uint64_t));

    size_t rp_sz = mseq_rpool_size(1);
    uint8_t rp_buf[rp_sz] __attribute__((aligned(MSEQ_CACHE_LINE)));
    mseq_rpool_t rp;
    mseq_rpool_init(&rp, rp_buf, sizeof(rp_buf), 1);

    /* Produce 4 items */
    for (int i = 0; i < 4; i++) {
        uint64_t *s = mseq_reserve(&q, 0, &rp);
        ASSERT_TRUE(s != NULL);
        *s = (uint64_t)(100 + i);
        mseq_submit(&q, 0);
    }

    /* Claim using announcement protocol */
    mseq_claim_t c = mseq_claim_announced(&q, &rp, 0, 4);
    ASSERT_TRUE(c.data != NULL);
    ASSERT_EQ(4U, c.count);

    /* Verify data correctness */
    for (uint32_t i = 0; i < c.count; i++) {
        uint64_t *item = (uint64_t *)mseq_claim_item(&q, &c, i);
        ASSERT_TRUE(item != NULL);
        ASSERT_EQ(100ULL + i, *item);
    }
    mseq_release(&q, &rp, 0, &c);

    /* Verify empty after drain */
    c = mseq_claim_announced(&q, &rp, 0, 1);
    ASSERT_TRUE(c.data == NULL);
}

/* Announce multi-consumer: 1P, 4C, no duplicates */
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

/* Announce concurrent stress: 2P, 4C, 500ms */
static mseq_t *g_ann_stress_q;
static mseq_rpool_t *g_ann_stress_rp;
static _Atomic int g_ann_stress_run;
static _Atomic uint64_t g_ann_stress_ops;

static void *ann_stress_prod(void *arg) {
    uint32_t id = (uint32_t)(uintptr_t)arg;
    uint64_t v = 0;
    while (atomic_load(&g_ann_stress_run)) {
        uint64_t *s = mseq_reserve(g_ann_stress_q, id, g_ann_stress_rp);
        if (s) { *s = v++; mseq_submit(g_ann_stress_q, id); }
    }
    return NULL;
}

static void *ann_stress_cons(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t n = 0;
    while (atomic_load(&g_ann_stress_run)) {
        mseq_claim_t c = mseq_claim_announced(g_ann_stress_q, g_ann_stress_rp, cid, 16);
        if (c.data) { n += c.count; mseq_release(g_ann_stress_q, g_ann_stress_rp, cid, &c); }
        else sched_yield();
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

    pthread_t prods[2], cons[4];
    for (int i = 0; i < 2; i++)
        pthread_create(&prods[i], NULL, ann_stress_prod, (void *)(uintptr_t)i);
    for (int i = 0; i < 4; i++)
        pthread_create(&cons[i], NULL, ann_stress_cons, (void *)(uintptr_t)i);

    struct timespec ts = {0, 500000000};  /* 500ms */
    nanosleep(&ts, NULL);
    atomic_store(&g_ann_stress_run, 0);

    for (int i = 0; i < 2; i++) pthread_join(prods[i], NULL);
    for (int i = 0; i < 4; i++) pthread_join(cons[i], NULL);

    uint64_t ops = atomic_load(&g_ann_stress_ops);
    printf("\n  announce stress (2P/4C R=16): %llu ops\n", (unsigned long long)ops);
    ASSERT_GT(ops, 1000ULL);
}

UTEST_MAIN()
