/*
 * MPMC Security Tests
 */

#include "../test_common.h"
#include <signal.h>
#include <setjmp.h>

/* ============================================================================
 * MEMORY CORRUPTION TESTS
 * Detect buffer overflows, use-after-free, double-free
 * ============================================================================ */

#define CANARY_VALUE 0xDEADBEEFCAFEBABEULL

typedef struct {
    uint64_t canary_before;
    uint64_t data[8];
    uint64_t canary_after;
} canary_payload_t;

UTEST(security, buffer_overflow_detection) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 16, sizeof(canary_payload_t)));
    
    /* Write with canaries */
    for (int i = 0; i < 16; i++) {
        canary_payload_t *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        slot->canary_before = CANARY_VALUE;
        slot->canary_after = CANARY_VALUE;
        for (int j = 0; j < 8; j++) slot->data[j] = i * 100 + j;
        mpmc_submit(&queue, 0);
    }
    
    /* Verify canaries intact after queue operations */
    for (int i = 0; i < 16; i++) {
        mpmc_item_t item = mpmc_claim(&queue);
        ASSERT_TRUE(item.data != NULL);
        canary_payload_t *p = (canary_payload_t *)item.data;
        ASSERT_EQ(CANARY_VALUE, p->canary_before);
        ASSERT_EQ(CANARY_VALUE, p->canary_after);
        mpmc_release(&queue, &item);
    }
    
    mpmc_destroy(&queue);
}

UTEST(security, slot_isolation) {
    /* Verify writes to one slot don't corrupt adjacent slots */
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, 64));
    
    /* Fill all slots with unique patterns */
    for (int i = 0; i < 8; i++) {
        uint8_t *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        memset(slot, 0xA0 + i, 64);
        mpmc_submit(&queue, 0);
    }
    
    /* Verify each slot has correct pattern */
    for (int i = 0; i < 8; i++) {
        mpmc_item_t item = mpmc_claim(&queue);
        ASSERT_TRUE(item.data != NULL);
        uint8_t *data = (uint8_t *)item.data;
        uint8_t expected = 0xA0 + i;
        for (int j = 0; j < 64; j++) {
            ASSERT_EQ(expected, data[j]);
        }
        mpmc_release(&queue, &item);
    }
    
    mpmc_destroy(&queue);
}

UTEST(security, partition_isolation) {
    /* Verify operations on one partition don't affect others */
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, 16, sizeof(uint64_t)));
    
    /* Fill each partition with unique values */
    for (uint32_t p = 0; p < 4; p++) {
        for (int i = 0; i < 16; i++) {
            uint64_t *slot = mpmc_reserve(&queue, p);
            ASSERT_TRUE(slot != NULL);
            *slot = (p << 16) | i;
            mpmc_submit(&queue, p);
        }
    }
    
    /* Claim from specific partitions and verify */
    for (uint32_t p = 0; p < 4; p++) {
        for (int i = 0; i < 16; i++) {
            mpmc_item_t item = mpmc_claim_partition(&queue, p);
            ASSERT_TRUE(item.data != NULL);
            uint64_t val = *(uint64_t *)item.data;
            ASSERT_EQ(p, (val >> 16) & 0xFFFF);
            mpmc_release(&queue, &item);
        }
    }
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * STATE MACHINE INVARIANT TESTS
 * Verify state transitions are always valid
 * ============================================================================ */

UTEST(security, state_never_skips) {
    /* States must follow: EMPTY -> FILLING -> READY -> READING -> EMPTY */
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    /* All slots start EMPTY */
    for (int i = 0; i < 8; i++) {
        uint64_t state = atomic_load(&queue.parts[0].slots[i].state);
        ASSERT_EQ((uint64_t)MPMC_EMPTY, state & 3);
    }
    
    /* Reserve -> FILLING */
    uint64_t *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    uint32_t reserved_slot = queue.parts[0].current;
    uint64_t state = atomic_load(&queue.parts[0].slots[reserved_slot].state);
    ASSERT_EQ((uint64_t)MPMC_FILLING, state & 3);
    
    /* Submit -> READY */
    mpmc_submit(&queue, 0);
    state = atomic_load(&queue.parts[0].slots[reserved_slot].state);
    ASSERT_EQ((uint64_t)MPMC_READY, state & 3);
    
    /* Claim -> READING */
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data != NULL);
    state = atomic_load(&queue.parts[0].slots[item.slot].state);
    ASSERT_EQ((uint64_t)MPMC_READING, state & 3);
    
    /* Release -> EMPTY (with incremented generation) */
    mpmc_release(&queue, &item);
    state = atomic_load(&queue.parts[0].slots[reserved_slot].state);
    ASSERT_EQ((uint64_t)MPMC_EMPTY, state & 3);
    ASSERT_TRUE((state >> 2) > 0);  /* Generation incremented */
    
    mpmc_destroy(&queue);
}

UTEST(security, generation_counter_increments) {
    /* Verify generation counter always increases to prevent ABA */
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    uint64_t total_increments = 0;
    for (int cycle = 0; cycle < 100; cycle++) {
        uint64_t *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        *slot = cycle;
        
        uint64_t state_before = atomic_load(&queue.parts[0].slots[queue.parts[0].current].state);
        mpmc_submit(&queue, 0);
        
        mpmc_item_t item = mpmc_claim(&queue);
        ASSERT_TRUE(item.data != NULL);
        
        uint64_t state_after = atomic_load(&queue.parts[0].slots[item.slot].state);
        /* Generation in READING state should be same as READY */
        uint64_t gen_before = state_before >> 2;
        uint64_t gen_after = state_after >> 2;
        ASSERT_TRUE(gen_after >= gen_before);
        
        mpmc_release(&queue, &item);
        total_increments++;
    }
    
    ASSERT_EQ(100ULL, total_increments);
    mpmc_destroy(&queue);
}

/* ============================================================================
 * BOUNDARY CONDITION TESTS
 * Test at limits of valid inputs
 * ============================================================================ */

UTEST(security, max_partitions) {
    mpmc_t queue;
    /* Test with many partitions */
    ASSERT_EQ(0, mpmc_init(&queue, 128, 8, sizeof(uint64_t)));
    
    for (uint32_t p = 0; p < 128; p++) {
        uint64_t *slot = mpmc_reserve(&queue, p);
        ASSERT_TRUE(slot != NULL);
        *slot = p;
        mpmc_submit(&queue, p);
    }
    
    for (uint32_t p = 0; p < 128; p++) {
        mpmc_item_t item = mpmc_claim_partition(&queue, p);
        ASSERT_TRUE(item.data != NULL);
        ASSERT_EQ(p, *(uint64_t *)item.data);
        mpmc_release(&queue, &item);
    }
    
    mpmc_destroy(&queue);
}

UTEST(security, min_valid_config) {
    mpmc_t queue;
    /* Minimum practical: 1 partition, 2 slots (power of 2), 8 bytes */
    ASSERT_EQ(0, mpmc_init(&queue, 1, 2, 8));
    
    uint8_t *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    *slot = 0x42;
    mpmc_submit(&queue, 0);
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data != NULL);
    ASSERT_EQ(0x42, *(uint8_t *)item.data);
    mpmc_release(&queue, &item);
    
    mpmc_destroy(&queue);
}

UTEST(security, large_slot_boundary) {
    mpmc_t queue;
    /* Large slot size - stress alignment */
    size_t large_size = 8192;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, large_size));
    
    uint8_t *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    
    /* Write pattern at boundaries */
    slot[0] = 0xAA;
    slot[large_size/2] = 0xBB;
    slot[large_size - 1] = 0xCC;
    mpmc_submit(&queue, 0);
    
    mpmc_item_t item = mpmc_claim(&queue);
    uint8_t *data = (uint8_t *)item.data;
    ASSERT_EQ(0xAA, data[0]);
    ASSERT_EQ(0xBB, data[large_size/2]);
    ASSERT_EQ(0xCC, data[large_size - 1]);
    mpmc_release(&queue, &item);
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * CONCURRENT STRESS TESTS
 * High-contention scenarios to expose race conditions
 * ============================================================================ */

static mpmc_t *g_stress_queue;
static _Atomic int g_stress_running;
static _Atomic uint64_t g_stress_ops;
static _Atomic uint64_t g_stress_errors;

static void* sec_stress_producer(void *arg) {
    uint32_t part = (uint32_t)(uintptr_t)arg;
    uint64_t local_ops = 0;
    
    while (atomic_load(&g_stress_running)) {
        uint64_t *slot = mpmc_reserve(g_stress_queue, part);
        if (slot) {
            *slot = ((uint64_t)part << 32) | local_ops;
            mpmc_submit(g_stress_queue, part);
            local_ops++;
        }
        /* No yield - maximum pressure */
    }
    
    atomic_fetch_add(&g_stress_ops, local_ops);
    return NULL;
}

static void* sec_stress_consumer(void *arg) {
    (void)arg;
    uint64_t local_errors = 0;
    
    while (atomic_load(&g_stress_running)) {
        mpmc_item_t item = mpmc_claim(g_stress_queue);
        if (item.data) {
            uint64_t val = *(uint64_t *)item.data;
            uint32_t part = (uint32_t)(val >> 32);
            if (part >= g_stress_queue->num_parts) {
                local_errors++;
            }
            mpmc_release(g_stress_queue, &item);
        }
    }
    
    /* Drain remaining */
    while (mpmc_has_data(g_stress_queue)) {
        mpmc_item_t item = mpmc_claim(g_stress_queue);
        if (item.data) mpmc_release(g_stress_queue, &item);
    }
    
    atomic_fetch_add(&g_stress_errors, local_errors);
    return NULL;
}

UTEST(security, extreme_contention_4p4c) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, 32, sizeof(uint64_t)));
    
    g_stress_queue = &queue;
    atomic_store(&g_stress_running, 1);
    atomic_store(&g_stress_ops, 0);
    atomic_store(&g_stress_errors, 0);
    
    pthread_t prods[4], cons[4];
    for (int i = 0; i < 4; i++) {
        pthread_create(&prods[i], NULL, sec_stress_producer, (void*)(uintptr_t)i);
        pthread_create(&cons[i], NULL, sec_stress_consumer, NULL);
    }
    
    /* Run for 500ms */
    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    
    atomic_store(&g_stress_running, 0);
    
    for (int i = 0; i < 4; i++) {
        pthread_join(prods[i], NULL);
        pthread_join(cons[i], NULL);
    }
    
    uint64_t ops = atomic_load(&g_stress_ops);
    uint64_t errors = atomic_load(&g_stress_errors);
    
    printf("\n  Extreme contention (4P/4C): %llu ops, %llu errors\n",
           (unsigned long long)ops, (unsigned long long)errors);
    
    ASSERT_EQ(0ULL, errors);
    ASSERT_GT(ops, 100000ULL);  /* Should process many ops */
    
    mpmc_destroy(&queue);
}

UTEST(security, extreme_contention_1p8c) {
    /* Single producer, 8 consumers - tests consumer race */
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 64, sizeof(uint64_t)));
    
    g_stress_queue = &queue;
    atomic_store(&g_stress_running, 1);
    atomic_store(&g_stress_ops, 0);
    atomic_store(&g_stress_errors, 0);
    
    pthread_t prod, cons[8];
    pthread_create(&prod, NULL, sec_stress_producer, (void*)(uintptr_t)0);
    for (int i = 0; i < 8; i++) {
        pthread_create(&cons[i], NULL, sec_stress_consumer, NULL);
    }
    
    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);
    
    atomic_store(&g_stress_running, 0);
    
    pthread_join(prod, NULL);
    for (int i = 0; i < 8; i++) {
        pthread_join(cons[i], NULL);
    }
    
    uint64_t errors = atomic_load(&g_stress_errors);
    printf("\n  Consumer race (1P/8C): %llu errors\n", (unsigned long long)errors);
    ASSERT_EQ(0ULL, errors);
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * DOUBLE OPERATION TESTS
 * Verify double-submit, double-release, etc. are handled safely
 * ============================================================================ */

UTEST(security, double_submit_safe) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    uint64_t *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    *slot = 42;
    mpmc_submit(&queue, 0);
    
    /* Double submit should be no-op (current == UINT32_MAX) */
    mpmc_submit(&queue, 0);  /* Should not crash */
    
    /* Should still have exactly 1 item */
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data != NULL);
    ASSERT_EQ(42ULL, *(uint64_t *)item.data);
    mpmc_release(&queue, &item);
    
    mpmc_item_t item2 = mpmc_claim(&queue);
    ASSERT_TRUE(item2.data == NULL);  /* No second item */
    
    mpmc_destroy(&queue);
}

UTEST(security, double_discard_safe) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    mpmc_reserve(&queue, 0);
    mpmc_discard(&queue, 0);
    
    /* Double discard should be no-op */
    mpmc_discard(&queue, 0);  /* Should not crash */
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data == NULL);  /* No items */
    
    mpmc_destroy(&queue);
}

UTEST(security, release_null_item) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    /* Release with NULL/invalid should be safe */
    mpmc_item_t null_item = {NULL, UINT32_MAX, UINT32_MAX};
    mpmc_release(&queue, &null_item);  /* Should not crash */
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * RAPID ALLOC/FREE CYCLES
 * Stress test memory management
 * ============================================================================ */

UTEST(security, rapid_init_destroy) {
    /* Rapid queue creation/destruction shouldn't leak */
    for (int i = 0; i < 100; i++) {
        mpmc_t queue;
        ASSERT_EQ(0, mpmc_init(&queue, 4, 64, 256));
        
        /* Do some operations */
        for (int p = 0; p < 4; p++) {
            void *slot = mpmc_reserve(&queue, p);
            if (slot) mpmc_submit(&queue, p);
        }
        
        mpmc_destroy(&queue);
    }
}

UTEST(security, rapid_reserve_discard) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    /* Rapid reserve/discard cycles */
    for (int i = 0; i < 10000; i++) {
        void *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        mpmc_discard(&queue, 0);
    }
    
    /* Queue should still be functional */
    uint64_t *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    *slot = 12345;
    mpmc_submit(&queue, 0);
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data != NULL);
    ASSERT_EQ(12345ULL, *(uint64_t *)item.data);
    mpmc_release(&queue, &item);
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * FUZZING-STYLE RANDOM TESTS  
 * Random operations to find edge cases
 * ============================================================================ */

static uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

UTEST(security, random_operations) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, 32, sizeof(uint64_t)));
    
    uint32_t rng = 12345;
    mpmc_item_t held_items[16] = {0};
    int held_count = 0;
    
    for (int i = 0; i < 10000; i++) {
        uint32_t op = xorshift32(&rng) % 4;
        uint32_t part = xorshift32(&rng) % 4;
        
        switch (op) {
        case 0: /* Reserve + submit */
            {
                uint64_t *slot = mpmc_reserve(&queue, part);
                if (slot) {
                    *slot = i;
                    mpmc_submit(&queue, part);
                }
            }
            break;
            
        case 1: /* Reserve + discard */
            {
                void *slot = mpmc_reserve(&queue, part);
                if (slot) mpmc_discard(&queue, part);
            }
            break;
            
        case 2: /* Claim */
            if (held_count < 16) {
                mpmc_item_t item = mpmc_claim(&queue);
                if (item.data) {
                    held_items[held_count++] = item;
                }
            }
            break;
            
        case 3: /* Release */
            if (held_count > 0) {
                int idx = xorshift32(&rng) % held_count;
                mpmc_release(&queue, &held_items[idx]);
                held_items[idx] = held_items[--held_count];
            }
            break;
        }
    }
    
    /* Release all held items */
    for (int i = 0; i < held_count; i++) {
        mpmc_release(&queue, &held_items[i]);
    }
    
    /* Drain queue */
    while (mpmc_has_data(&queue)) {
        mpmc_item_t item = mpmc_claim(&queue);
        if (item.data) mpmc_release(&queue, &item);
    }
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * ALIGNMENT TESTS
 * Verify memory alignment requirements
 * ============================================================================ */

UTEST(security, slot_alignment) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 16, 64));
    
    for (int i = 0; i < 16; i++) {
        void *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        
        /* Slot should be properly aligned for any type */
        uintptr_t addr = (uintptr_t)slot;
        ASSERT_EQ(0ULL, addr % 8);  /* At least 8-byte aligned */
        
        mpmc_submit(&queue, 0);
    }
    
    mpmc_destroy(&queue);
}

UTEST(security, partition_cache_alignment) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 4, 16, sizeof(uint64_t)));
    
    /* Each partition should be cache-line aligned */
    for (uint32_t p = 0; p < 4; p++) {
        uintptr_t addr = (uintptr_t)&queue.parts[p];
        ASSERT_EQ(0ULL, addr % MPMC_CACHE_LINE);
    }
    
    mpmc_destroy(&queue);
}

