/*
 * MPMC Concurrent Tests
 */

#include "../test_common.h"

/* ============================================================================
 * SPSC Test
 * ============================================================================ */

static mpmc_t *g_spsc_queue;
static _Atomic int g_spsc_stop;
static _Atomic int g_spsc_produced;
static _Atomic int g_spsc_consumed;

static void *spsc_producer(void *arg) {
    (void)arg;
    for (int i = 0; i < 10000; i++) {
        void *slot;
        while ((slot = mpmc_reserve(g_spsc_queue, 0)) == NULL) {
            sched_yield();
        }
        *(int *)slot = i;
        mpmc_submit(g_spsc_queue, 0);
        atomic_fetch_add(&g_spsc_produced, 1);
    }
    return NULL;
}

static void *spsc_consumer(void *arg) {
    (void)arg;
    while (!atomic_load(&g_spsc_stop) || mpmc_has_data(g_spsc_queue)) {
        mpmc_item_t item = mpmc_claim(g_spsc_queue);
        if (item.data) {
            atomic_fetch_add(&g_spsc_consumed, 1);
            mpmc_release(g_spsc_queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mpmc_concurrent, single_producer_single_consumer) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    g_spsc_queue = &queue;
    atomic_store(&g_spsc_stop, 0);
    atomic_store(&g_spsc_produced, 0);
    atomic_store(&g_spsc_consumed, 0);
    
    pthread_t prod, cons;
    pthread_create(&prod, NULL, spsc_producer, NULL);
    pthread_create(&cons, NULL, spsc_consumer, NULL);
    
    pthread_join(prod, NULL);
    atomic_store(&g_spsc_stop, 1);
    pthread_join(cons, NULL);
    
    ASSERT_EQ(10000, atomic_load(&g_spsc_produced));
    ASSERT_EQ(10000, atomic_load(&g_spsc_consumed));
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * MPSC Test
 * ============================================================================ */

static mpmc_t *g_mpsc_queue;
static _Atomic int g_mpsc_stop;
static _Atomic int g_mpsc_produced;
static _Atomic int g_mpsc_consumed;

static void *mpsc_producer(void *arg) {
    uint32_t part = (uint32_t)(uintptr_t)arg;
    for (int i = 0; i < 2500; i++) {
        void *slot;
        while ((slot = mpmc_reserve(g_mpsc_queue, part)) == NULL) {
            sched_yield();
        }
        *(int *)slot = i;
        mpmc_submit(g_mpsc_queue, part);
        atomic_fetch_add(&g_mpsc_produced, 1);
    }
    return NULL;
}

static void *mpsc_consumer(void *arg) {
    (void)arg;
    while (!atomic_load(&g_mpsc_stop) || mpmc_has_data(g_mpsc_queue)) {
        mpmc_item_t item = mpmc_claim(g_mpsc_queue);
        if (item.data) {
            atomic_fetch_add(&g_mpsc_consumed, 1);
            mpmc_release(g_mpsc_queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mpmc_concurrent, multi_producer_single_consumer) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, TEST_SLOTS, TEST_SLOT_SIZE));
    
    g_mpsc_queue = &queue;
    atomic_store(&g_mpsc_stop, 0);
    atomic_store(&g_mpsc_produced, 0);
    atomic_store(&g_mpsc_consumed, 0);
    
    pthread_t prods[4], cons;
    for (int i = 0; i < 4; i++) {
        pthread_create(&prods[i], NULL, mpsc_producer, (void *)(uintptr_t)i);
    }
    pthread_create(&cons, NULL, mpsc_consumer, NULL);
    
    for (int i = 0; i < 4; i++) {
        pthread_join(prods[i], NULL);
    }
    atomic_store(&g_mpsc_stop, 1);
    pthread_join(cons, NULL);
    
    ASSERT_EQ(10000, atomic_load(&g_mpsc_produced));
    ASSERT_EQ(10000, atomic_load(&g_mpsc_consumed));
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * MPMC Test
 * ============================================================================ */

static mpmc_t *g_mpmc_queue;
static _Atomic int g_mpmc_stop;
static _Atomic int g_mpmc_produced;
static _Atomic int g_mpmc_consumed;

static void *mpmc_producer_thread(void *arg) {
    uint32_t part = (uint32_t)(uintptr_t)arg;
    for (int i = 0; i < 2500; i++) {
        void *slot;
        while ((slot = mpmc_reserve(g_mpmc_queue, part)) == NULL) {
            sched_yield();
        }
        *(int *)slot = i;
        mpmc_submit(g_mpmc_queue, part);
        atomic_fetch_add(&g_mpmc_produced, 1);
    }
    return NULL;
}

static void *mpmc_consumer_thread(void *arg) {
    (void)arg;
    while (!atomic_load(&g_mpmc_stop) || mpmc_has_data(g_mpmc_queue)) {
        mpmc_item_t item = mpmc_claim(g_mpmc_queue);
        if (item.data) {
            atomic_fetch_add(&g_mpmc_consumed, 1);
            mpmc_release(g_mpmc_queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mpmc_concurrent, multi_producer_multi_consumer) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, TEST_SLOTS, TEST_SLOT_SIZE));
    
    g_mpmc_queue = &queue;
    atomic_store(&g_mpmc_stop, 0);
    atomic_store(&g_mpmc_produced, 0);
    atomic_store(&g_mpmc_consumed, 0);
    
    pthread_t prods[4], cons[2];
    for (int i = 0; i < 4; i++) {
        pthread_create(&prods[i], NULL, mpmc_producer_thread, (void *)(uintptr_t)i);
    }
    for (int i = 0; i < 2; i++) {
        pthread_create(&cons[i], NULL, mpmc_consumer_thread, NULL);
    }
    
    for (int i = 0; i < 4; i++) {
        pthread_join(prods[i], NULL);
    }
    atomic_store(&g_mpmc_stop, 1);
    for (int i = 0; i < 2; i++) {
        pthread_join(cons[i], NULL);
    }
    
    ASSERT_EQ(10000, atomic_load(&g_mpmc_produced));
    ASSERT_EQ(10000, atomic_load(&g_mpmc_consumed));
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * Stress Test
 * ============================================================================ */

static mpmc_t *g_stress_queue;
static _Atomic int g_stress_stop;
static _Atomic int g_stress_produced;
static _Atomic int g_stress_consumed;

static void *stress_producer(void *arg) {
    uint32_t part = (uint32_t)(uintptr_t)arg;
    int n = 0;
    for (int i = 0; i < 5000; i++) {
        void *slot;
        while ((slot = mpmc_reserve(g_stress_queue, part)) == NULL) {
            sched_yield();
        }
        *(int *)slot = i;
        mpmc_submit(g_stress_queue, part);
        n++;
    }
    atomic_fetch_add(&g_stress_produced, n);
    return NULL;
}

static void *stress_consumer(void *arg) {
    (void)arg;
    int n = 0;
    while (!atomic_load(&g_stress_stop) || mpmc_has_data(g_stress_queue)) {
        mpmc_item_t item = mpmc_claim(g_stress_queue);
        if (item.data) {
            n++;
            mpmc_release(g_stress_queue, &item);
        } else {
            sched_yield();
        }
    }
    atomic_fetch_add(&g_stress_consumed, n);
    return NULL;
}

UTEST(mpmc_stress, high_contention) {
    mpmc_t queue;
    /* Small buffer = high contention */
    ASSERT_EQ(0, mpmc_init(&queue, 8, 32, TEST_SLOT_SIZE));
    
    g_stress_queue = &queue;
    atomic_store(&g_stress_stop, 0);
    atomic_store(&g_stress_produced, 0);
    atomic_store(&g_stress_consumed, 0);
    
    pthread_t prods[8], cons[4];
    for (int i = 0; i < 8; i++) {
        pthread_create(&prods[i], NULL, stress_producer, (void *)(uintptr_t)i);
    }
    for (int i = 0; i < 4; i++) {
        pthread_create(&cons[i], NULL, stress_consumer, NULL);
    }
    
    for (int i = 0; i < 8; i++) {
        pthread_join(prods[i], NULL);
    }
    atomic_store(&g_stress_stop, 1);
    for (int i = 0; i < 4; i++) {
        pthread_join(cons[i], NULL);
    }
    
    ASSERT_EQ(40000, atomic_load(&g_stress_produced));
    ASSERT_EQ(40000, atomic_load(&g_stress_consumed));
    
    mpmc_destroy(&queue);
}
