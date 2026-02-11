/*
 * MPMC Edge Case Tests
 */

#include "../test_common.h"

/* ============================================================================
 * Parameter Validation Tests
 * ============================================================================ */

UTEST(mpmc_edge, init_zero_slots) {
    mpmc_t queue;
    /* slots=0 should fail */
    ASSERT_EQ(-1, mpmc_init(&queue, 1, 0, sizeof(uint64_t)));
}

UTEST(mpmc_edge, init_zero_partitions) {
    mpmc_t queue;
    /* num_parts=0 should fail */
    ASSERT_EQ(-1, mpmc_init(&queue, 0, 16, sizeof(uint64_t)));
}

UTEST(mpmc_edge, init_zero_slot_size) {
    mpmc_t queue;
    /* slot_size=0 should fail */
    ASSERT_EQ(-1, mpmc_init(&queue, 1, 16, 0));
}

UTEST(mpmc_edge, init_non_power_of_two) {
    mpmc_t queue;
    /* slots=7 (not power of 2) should fail */
    ASSERT_EQ(-1, mpmc_init(&queue, 1, 7, sizeof(uint64_t)));
    ASSERT_EQ(-1, mpmc_init(&queue, 1, 15, sizeof(uint64_t)));
    ASSERT_EQ(-1, mpmc_init(&queue, 1, 100, sizeof(uint64_t)));
    /* But power of 2 should succeed */
    ASSERT_EQ(0, mpmc_init(&queue, 1, 16, sizeof(uint64_t)));
    mpmc_destroy(&queue);
}

UTEST(mpmc_edge, release_invalid_item) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 2, 16, sizeof(uint64_t)));
    
    /* Release with invalid partition should not crash */
    mpmc_item_t bad_item = {NULL, 999, 0};
    mpmc_release(&queue, &bad_item);  /* Should return safely */
    
    /* Release with invalid slot should not crash */
    bad_item.part = 0;
    bad_item.slot = 999;
    mpmc_release(&queue, &bad_item);  /* Should return safely */
    
    mpmc_destroy(&queue);
}

/* ============================================================================
 * Original Edge Tests
 * ============================================================================ */

UTEST(mpmc_edge, small_partition) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 8, sizeof(uint64_t)));
    
    /* Fill all 8 slots */
    for (int i = 0; i < 8; i++) {
        void *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        *(int *)slot = i;
        mpmc_submit(&queue, 0);
    }
    
    ASSERT_TRUE(mpmc_reserve(&queue, 0) == NULL);
    
    for (int i = 0; i < 8; i++) {
        mpmc_item_t item = mpmc_claim(&queue);
        ASSERT_TRUE(item.data != NULL);
        ASSERT_EQ(i, *(int *)item.data);
        mpmc_release(&queue, &item);
    }
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data == NULL);
    
    mpmc_destroy(&queue);
}

UTEST(mpmc_edge, large_slot_size) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 16, 4096));
    
    void *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    
    /* Write pattern to large slot */
    memset(slot, 0xAB, 4096);
    mpmc_submit(&queue, 0);
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data != NULL);
    
    /* Verify pattern */
    uint8_t *data = (uint8_t *)item.data;
    for (int i = 0; i < 4096; i++) {
        ASSERT_EQ(0xAB, data[i]);
    }
    
    mpmc_release(&queue, &item);
    mpmc_destroy(&queue);
}

UTEST(mpmc_edge, many_partitions) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 64, 16, TEST_SLOT_SIZE));
    
    /* Add one item to each partition */
    for (uint32_t p = 0; p < 64; p++) {
        void *slot = mpmc_reserve(&queue, p);
        ASSERT_TRUE(slot != NULL);
        *(uint64_t *)slot = p;
        mpmc_submit(&queue, p);
    }
    
    /* Claim all - should see all partitions */
    uint64_t seen = 0;
    for (int i = 0; i < 64; i++) {
        mpmc_item_t item = mpmc_claim(&queue);
        ASSERT_TRUE(item.data != NULL);
        uint64_t p = *(uint64_t *)item.data;
        seen |= (1ULL << p);
        mpmc_release(&queue, &item);
    }
    
    ASSERT_EQ(0xFFFFFFFFFFFFFFFFULL, seen);
    
    mpmc_destroy(&queue);
}
