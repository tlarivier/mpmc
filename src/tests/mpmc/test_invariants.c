/*
 * MPMC State Machine Invariant Tests
 */

#include "../test_common.h"

UTEST(mpmc_invariants, double_reserve_fails) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    void *slot1 = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot1 != NULL);
    
    /* Second reserve on same partition should fail (slot in FILLING state) */
    void *slot2 = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot2 == NULL);
    
    mpmc_discard(&queue, 0);
    mpmc_destroy(&queue);
}

UTEST(mpmc_invariants, state_transitions) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 16, TEST_SLOT_SIZE));
    
    /* EMPTY -> FILLING (reserve) */
    void *slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    
    /* FILLING -> READY (submit) */
    mpmc_submit(&queue, 0);
    
    /* READY -> READING (claim) */
    mpmc_item_t item = mpmc_claim(&queue);
    ASSERT_TRUE(item.data != NULL);
    
    /* READING -> EMPTY (release) */
    mpmc_release(&queue, &item);
    
    /* Back to EMPTY - can reserve again */
    slot = mpmc_reserve(&queue, 0);
    ASSERT_TRUE(slot != NULL);
    mpmc_discard(&queue, 0);
    
    mpmc_destroy(&queue);
}

UTEST(mpmc_invariants, fifo_order_per_partition) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, TEST_SLOTS, TEST_SLOT_SIZE));
    
    /* Enqueue in order */
    for (int i = 0; i < 10; i++) {
        void *slot = mpmc_reserve(&queue, 0);
        *(int *)slot = i;
        mpmc_submit(&queue, 0);
    }
    
    /* Dequeue should be in same order */
    for (int i = 0; i < 10; i++) {
        mpmc_item_t item = mpmc_claim(&queue);
        ASSERT_EQ(i, *(int *)item.data);
        mpmc_release(&queue, &item);
    }
    
    mpmc_destroy(&queue);
}

