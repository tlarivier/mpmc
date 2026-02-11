/*
 * MPMC Critical Tests
 */

#include "../test_common.h"

/* ============================================================================
 * DUPLICATION / LOSS DETECTION
 * Each item has unique ID, verify all seen exactly once
 * ============================================================================ */

#define DEDUP_PRODUCERS 4
#define DEDUP_ITEMS_PER_PROD 10000
#define DEDUP_TOTAL (DEDUP_PRODUCERS * DEDUP_ITEMS_PER_PROD)

static mpmc_t *g_dedup_queue;
static _Atomic uint8_t g_seen[DEDUP_TOTAL];
static _Atomic int g_dedup_duplicates;
static _Atomic int g_dedup_consumed;
static _Atomic int g_dedup_stop;

static void *dedup_producer(void *arg) {
    uint32_t prod_id = (uint32_t)(uintptr_t)arg;
    for (int i = 0; i < DEDUP_ITEMS_PER_PROD; i++) {
        uint64_t *slot;
        while ((slot = mpmc_reserve(g_dedup_queue, prod_id)) == NULL) {
            sched_yield();
        }
        *slot = (uint64_t)prod_id * DEDUP_ITEMS_PER_PROD + i;
        mpmc_submit(g_dedup_queue, prod_id);
    }
    return NULL;
}

static void *dedup_consumer(void *arg) {
    (void)arg;
    while (!atomic_load(&g_dedup_stop) || mpmc_has_data(g_dedup_queue)) {
        mpmc_item_t item = mpmc_claim(g_dedup_queue);
        if (item.data) {
            uint64_t id = *(uint64_t *)item.data;
            if (id < DEDUP_TOTAL) {
                uint8_t prev = atomic_exchange(&g_seen[id], 1);
                if (prev != 0) {
                    atomic_fetch_add(&g_dedup_duplicates, 1);
                }
            }
            atomic_fetch_add(&g_dedup_consumed, 1);
            mpmc_release(g_dedup_queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mpmc_critical, no_duplication_no_loss) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, DEDUP_PRODUCERS, 256, sizeof(uint64_t)));
    
    g_dedup_queue = &queue;
    memset((void *)g_seen, 0, sizeof(g_seen));
    atomic_store(&g_dedup_duplicates, 0);
    atomic_store(&g_dedup_consumed, 0);
    atomic_store(&g_dedup_stop, 0);
    
    pthread_t prods[DEDUP_PRODUCERS], cons[2];
    
    for (int i = 0; i < DEDUP_PRODUCERS; i++) {
        pthread_create(&prods[i], NULL, dedup_producer, (void *)(uintptr_t)i);
    }
    for (int i = 0; i < 2; i++) {
        pthread_create(&cons[i], NULL, dedup_consumer, NULL);
    }
    
    for (int i = 0; i < DEDUP_PRODUCERS; i++) {
        pthread_join(prods[i], NULL);
    }
    atomic_store(&g_dedup_stop, 1);
    for (int i = 0; i < 2; i++) {
        pthread_join(cons[i], NULL);
    }
    
    ASSERT_EQ(0, atomic_load(&g_dedup_duplicates));
    ASSERT_EQ(DEDUP_TOTAL, atomic_load(&g_dedup_consumed));
    
    int missing = 0;
    for (int i = 0; i < DEDUP_TOTAL; i++) {
        if (atomic_load(&g_seen[i]) != 1) missing++;
    }
    ASSERT_EQ(0, missing);
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * ABA PROTECTION TEST
 * Small buffer, high churn - generation counter must prevent ABA
 * ============================================================================ */

#define ABA_ITERATIONS 50000

static mpmc_t *g_aba_queue;
static _Atomic uint8_t g_aba_seen[ABA_ITERATIONS];
static _Atomic int g_aba_produced;
static _Atomic int g_aba_consumed;
static _Atomic int g_aba_duplicates;
static _Atomic int g_aba_stop;

static void *aba_producer(void *arg) {
    (void)arg;
    for (int i = 0; i < ABA_ITERATIONS; i++) {
        uint64_t *slot;
        while ((slot = mpmc_reserve(g_aba_queue, 0)) == NULL) {
            sched_yield();
        }
        *slot = (uint64_t)i;
        mpmc_submit(g_aba_queue, 0);
        atomic_fetch_add(&g_aba_produced, 1);
    }
    return NULL;
}

static void *aba_consumer(void *arg) {
    (void)arg;
    while (!atomic_load(&g_aba_stop) || mpmc_has_data(g_aba_queue)) {
        mpmc_item_t item = mpmc_claim(g_aba_queue);
        if (item.data) {
            uint64_t val = *(uint64_t *)item.data;
            if (val < ABA_ITERATIONS) {
                uint8_t prev = atomic_exchange(&g_aba_seen[val], 1);
                if (prev != 0) {
                    atomic_fetch_add(&g_aba_duplicates, 1);
                }
            }
            atomic_fetch_add(&g_aba_consumed, 1);
            mpmc_release(g_aba_queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mpmc_critical, aba_high_churn) {
    mpmc_t queue;
    /* 8 slots = high reuse rate = stress test for generation counter */
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    g_aba_queue = &queue;
    memset((void *)g_aba_seen, 0, sizeof(g_aba_seen));
    atomic_store(&g_aba_produced, 0);
    atomic_store(&g_aba_consumed, 0);
    atomic_store(&g_aba_duplicates, 0);
    atomic_store(&g_aba_stop, 0);
    
    pthread_t prod, cons;
    pthread_create(&prod, NULL, aba_producer, NULL);
    pthread_create(&cons, NULL, aba_consumer, NULL);
    
    pthread_join(prod, NULL);
    atomic_store(&g_aba_stop, 1);
    pthread_join(cons, NULL);
    
    ASSERT_EQ(0, atomic_load(&g_aba_duplicates));
    ASSERT_EQ(ABA_ITERATIONS, atomic_load(&g_aba_produced));
    ASSERT_EQ(ABA_ITERATIONS, atomic_load(&g_aba_consumed));
    
    int missing = 0;
    for (int i = 0; i < ABA_ITERATIONS; i++) {
        if (atomic_load(&g_aba_seen[i]) != 1) missing++;
    }
    ASSERT_EQ(0, missing);
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * CONSUMER RACE - Only one claim succeeds
 * ============================================================================ */

#define RACE_ITERATIONS 10000

static mpmc_t *g_race_queue;
static _Atomic int g_race_claims;
static _Atomic int g_race_round;

static void *race_consumer(void *arg) {
    (void)arg;
    for (int round = 0; round < RACE_ITERATIONS; round++) {
        while (atomic_load(&g_race_round) < round) {
            sched_yield();
        }
        
        mpmc_item_t item = mpmc_claim(g_race_queue);
        if (item.data) {
            atomic_fetch_add(&g_race_claims, 1);
            mpmc_release(g_race_queue, &item);
        }
    }
    return NULL;
}

UTEST(mpmc_critical, consumer_race_single_item) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 16, sizeof(uint64_t)));
    
    g_race_queue = &queue;
    atomic_store(&g_race_claims, 0);
    atomic_store(&g_race_round, -1);
    
    pthread_t cons[4];
    for (int i = 0; i < 4; i++) {
        pthread_create(&cons[i], NULL, race_consumer, NULL);
    }
    
    int successful_rounds = 0;
    for (int round = 0; round < RACE_ITERATIONS; round++) {
        uint64_t *slot = mpmc_reserve(&queue, 0);
        if (slot) {
            *slot = round;
            mpmc_submit(&queue, 0);
            successful_rounds++;
        }
        
        atomic_store(&g_race_round, round);
        
        for (volatile int j = 0; j < 100; j++) {}
    }
    
    atomic_store(&g_race_round, RACE_ITERATIONS);
    for (int i = 0; i < 4; i++) {
        pthread_join(cons[i], NULL);
    }
    
    ASSERT_EQ(successful_rounds, atomic_load(&g_race_claims));
    
    mpmc_destroy(&queue);
}
