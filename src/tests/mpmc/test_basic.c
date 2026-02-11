/*
 * MPMC Basic Functionality Tests
 */

#include "../test_common.h"

UTEST(mpmc_basic, init_destroy) {
    mpmc_t queue;
    int ret = mpmc_init(&queue, TEST_PARTITIONS, TEST_SLOTS, TEST_SLOT_SIZE);
    ASSERT_EQ(0, ret);
    ASSERT_EQ((uint32_t)TEST_PARTITIONS, queue.num_parts);
    
    mpmc_destroy(&queue);
}

UTEST(mpmc_basic, init_invalid_params) {
    mpmc_t queue;
    
    /* Non-power-of-2 slots must fail */
    ASSERT_NE(0, mpmc_init(&queue, TEST_PARTITIONS, 63, TEST_SLOT_SIZE));
    ASSERT_NE(0, mpmc_init(&queue, TEST_PARTITIONS, 17, TEST_SLOT_SIZE));
    
    /* Power-of-2 slots should succeed */
    ASSERT_EQ(0, mpmc_init(&queue, TEST_PARTITIONS, 32, TEST_SLOT_SIZE));
    mpmc_destroy(&queue);
}

UTEST(mpmc_basic, reserve_submit_claim_release) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    void *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    
    uint64_t *data = (uint64_t *)slot;
    *data = 0xDEADBEEF;
    
    mpmc_submit(&queue, 0);
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data != NULL);
    ASSERT_EQ(0xDEADBEEF, *(uint64_t *)item.data);
    
    mpmc_release(&queue, &item);
    mpmc_destroy(&queue);
}

UTEST(mpmc_basic, reserve_discard) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    void *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    mpmc_discard(&queue, 0);
    
    slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    
    mpmc_discard(&queue, 0);
    mpmc_destroy(&queue);
}

UTEST(mpmc_basic, claim_empty_queue) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data == NULL);
    
    mpmc_destroy(&queue);
}

UTEST(mpmc_basic, invalid_partition) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 2, TEST_SLOTS, TEST_SLOT_SIZE));
    
    /* Valid partitions */
    ASSERT_TRUE(mpmc_reserve(&queue, 0) != NULL);
    mpmc_discard(&queue, 0);
    ASSERT_TRUE(mpmc_reserve(&queue, 1) != NULL);
    mpmc_discard(&queue, 1);
    
    /* Invalid partition */
    ASSERT_TRUE(mpmc_reserve(&queue, 2) == NULL);
    ASSERT_TRUE(mpmc_reserve(&queue, 99) == NULL);
    
    mpmc_destroy(&queue);
}
