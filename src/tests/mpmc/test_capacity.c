/*
 * MPMC Capacity and Batch Tests
 */

#include "../test_common.h"

UTEST(mpmc_capacity, fill_partition) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    /* Fill entire partition */
    for (int i = 0; i < TEST_SLOTS; i++) {
        void *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        *(int *)slot = i;
        mpmc_submit(&queue, 0);
    }
    
    /* Should be full */
    ASSERT_TRUE(mpmc_reserve(&queue, 0) == NULL);
    
    /* Drain and verify */
    for (int i = 0; i < TEST_SLOTS; i++) {
        mpmc_item_t item = mpmc_claim(&queue);
        ASSERT_TRUE(item.data != NULL);
        ASSERT_EQ(i, *(int *)item.data);
        mpmc_release(&queue, &item);
    }
    
    mpmc_destroy(&queue);
}

UTEST(mpmc_capacity, multi_partition_fill) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, TEST_PARTITIONS, TEST_SLOTS, TEST_SLOT_SIZE));
    
    /* Fill each partition */
    for (uint32_t p = 0; p < TEST_PARTITIONS; p++) {
        for (int i = 0; i < TEST_SLOTS; i++) {
            void *slot = mpmc_reserve(&queue, p);
            ASSERT_TRUE(slot != NULL);
            *(int *)slot = p * 1000 + i;
            mpmc_submit(&queue, p);
        }
        ASSERT_TRUE(mpmc_reserve(&queue, p) == NULL);
    }
    
    /* Drain all */
    int count = 0;
    mpmc_item_t item;
    while ((item = mpmc_claim(&queue)).data != NULL) {
        count++;
        mpmc_release(&queue, &item);
    }
    
    ASSERT_EQ(TEST_PARTITIONS * TEST_SLOTS, count);
    
    mpmc_destroy(&queue);
}

UTEST(mpmc_batch, claim_batch) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    /* Add 32 items */
    for (int i = 0; i < 32; i++) {
        void *slot = mpmc_reserve(&queue, 0);
        ASSERT_TRUE(slot != NULL);
        *(int *)slot = i;
        mpmc_submit(&queue, 0);
    }
    
    /* Claim batch */
    mpmc_batch_t batch;
    int claimed = mpmc_claim_batch(&queue, &batch, 64);
    ASSERT_EQ(32, claimed);
    
    /* Verify all items in batch */
    for (int i = 0; i < claimed; i++) {
        ASSERT_TRUE(batch.items[i].data != NULL);
    }
    
    mpmc_release_batch(&queue, &batch);
    mpmc_destroy(&queue);
}

UTEST(mpmc_batch, claim_batch_empty) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    mpmc_batch_t batch;
    int claimed = mpmc_claim_batch(&queue, &batch, 64);
    ASSERT_EQ(0, claimed);
    
    mpmc_destroy(&queue);
}
