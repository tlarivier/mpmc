/*
 * MPMC Gap Tests - Core queue coverage gaps
 */

#include "../test_common.h"

/* ============================================================================
 * OVERFLOW BUILTIN TESTS
 * ============================================================================ */

UTEST(mpmc_gaps, overflow_slot_size_mul) {
    mpmc_t queue;
    ASSERT_EQ(-1, mpmc_init(&queue, 1, 1024, UINT32_MAX));
}

UTEST(mpmc_gaps, overflow_slots_total_mul) {
    mpmc_t queue;
    int result = mpmc_init(&queue, 0x1000, 0x1000, 0x1000);
    ASSERT_EQ(-1, result);
}

UTEST(mpmc_gaps, overflow_boundary_valid) {
    mpmc_t queue;
    int result = mpmc_init(&queue, 4, 64, 1024);
    ASSERT_EQ(0, result);
    mpmc_destroy(&queue);
}

/* ============================================================================
 * CLAIM_PARTITION ISOLATION
 * ============================================================================ */

UTEST(mpmc_gaps, claim_partition_isolation) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, 16, sizeof(uint64_t)));
    
    for (uint32_t p = 0; p < 3; p++) {
        for (int i = 0; i < 8; i++) {
            uint64_t *slot = mpmc_reserve(&queue, p);
            ASSERT_TRUE(slot != NULL);
            *slot = (p << 16) | i;
            mpmc_submit(&queue, p);
        }
    }
    
    int p1_count = 0;
    for (int i = 0; i < 20; i++) {
        mpmc_item_t item = mpmc_claim_partition(&queue, 1);
        if (item.data) {
            uint64_t val = *(uint64_t *)item.data;
            ASSERT_EQ(1U, (val >> 16) & 0xFFFF);
            p1_count++;
            mpmc_release(&queue, &item);
        }
    }
    ASSERT_EQ(8, p1_count);
    
    for (uint32_t p = 0; p < 3; p += 2) {
        int count = 0;
        for (int i = 0; i < 16; i++) {
            mpmc_item_t item = mpmc_claim_partition(&queue, p);
            if (item.data) {
                count++;
                mpmc_release(&queue, &item);
            }
        }
        ASSERT_EQ(8, count);
    }
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * RELEASE_BATCH
 * ============================================================================ */

UTEST(mpmc_gaps, release_batch_all) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 32, sizeof(uint64_t)));
    
    for (int i = 0; i < 16; i++) {
        uint64_t *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        *slot = i;
        mpmc_submit(&queue, 0);
    }
    
    mpmc_batch_t batch;
    int claimed = mpmc_claim_batch(&queue, &batch, 16);
    ASSERT_EQ(16, claimed);
    
    for (int i = 0; i < 16; i++) {
        ASSERT_TRUE(batch.items[i].data != NULL);
    }
    
    mpmc_release_batch(&queue, &batch);
    ASSERT_EQ(0U, batch.count);
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data == NULL);
    
    uint64_t *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    mpmc_discard(&queue, 0);
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * DISCARD CONCURRENT
 * ============================================================================ */

static mpmc_t *g_discard_queue;
static _Atomic int g_discard_running;
static _Atomic uint64_t g_discard_count;

static void* discard_producer(void *arg) {
    uint32_t part = (uint32_t)(uintptr_t)arg;
    while (atomic_load(&g_discard_running)) {
        void *slot = mpmc_reserve(g_discard_queue, part);
        if (slot) {
            if ((atomic_fetch_add(&g_discard_count, 1) % 2) == 0) {
                mpmc_submit(g_discard_queue, part);
            } else {
                mpmc_discard(g_discard_queue, part);
            }
        }
    }
    return NULL;
}

static void* discard_consumer(void *arg) {
    (void)arg;
    while (atomic_load(&g_discard_running)) {
        mpmc_item_t item = mpmc_claim(g_discard_queue);
        if (item.data) mpmc_release(g_discard_queue, &item);
    }
    while (mpmc_has_data(g_discard_queue)) {
        mpmc_item_t item = mpmc_claim(g_discard_queue);
        if (item.data) mpmc_release(g_discard_queue, &item);
    }
    return NULL;
}

UTEST(mpmc_gaps, discard_concurrent) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, 32, sizeof(uint64_t)));
    
    g_discard_queue = &queue;
    atomic_store(&g_discard_running, 1);
    atomic_store(&g_discard_count, 0);
    
    pthread_t prods[4], cons;
    for (int i = 0; i < 4; i++) {
        pthread_create(&prods[i], NULL, discard_producer, (void*)(uintptr_t)i);
    }
    pthread_create(&cons, NULL, discard_consumer, NULL);
    
    struct timespec ts = {0, 200000000};
    nanosleep(&ts, NULL);
    
    atomic_store(&g_discard_running, 0);
    
    pthread_join(cons, NULL);
    for (int i = 0; i < 4; i++) {
        pthread_join(prods[i], NULL);
    }
    
    uint64_t ops = atomic_load(&g_discard_count);
    printf("\n  Discard concurrent: %llu ops\n", (unsigned long long)ops);
    ASSERT_GT(ops, 1000ULL);
    
    mpmc_destroy(&queue);
}
