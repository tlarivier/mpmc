/*
 * MPMC Memory Ordering Tests
 */

#include "../test_common.h"

typedef struct {
    uint64_t seq;
    uint64_t data[7];  /* Fill to 64 bytes */
} ordered_msg_t;

static mpmc_t *g_order_queue;
static _Atomic int g_order_errors;
static _Atomic int g_order_consumed;
static _Atomic int g_order_stop;

static void *order_producer(void *arg) {
    (void)arg;
    for (uint64_t i = 0; i < 10000; i++) {
        ordered_msg_t *slot;
        while ((slot = mpmc_reserve(g_order_queue, 0)) == NULL) {
            sched_yield();
        }
        slot->seq = i;
        for (int j = 0; j < 7; j++) {
            slot->data[j] = i * 100 + j;
        }
        mpmc_submit(g_order_queue, 0);
    }
    return NULL;
}

static void *order_consumer(void *arg) {
    (void)arg;
    while (!atomic_load(&g_order_stop) || mpmc_has_data(g_order_queue)) {
        mpmc_item_t item = mpmc_claim(g_order_queue);
        if (item.data) {
            ordered_msg_t *msg = (ordered_msg_t *)item.data;
            for (int j = 0; j < 7; j++) {
                if (msg->data[j] != msg->seq * 100 + (uint64_t)j) {
                    atomic_fetch_add(&g_order_errors, 1);
                }
            }
            atomic_fetch_add(&g_order_consumed, 1);
            mpmc_release(g_order_queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mpmc_ordering, data_visibility) {
    mpmc_t queue;
    ASSERT_EQ(0, mpmc_init(&queue, 1, 64, sizeof(ordered_msg_t)));
    
    g_order_queue = &queue;
    atomic_store(&g_order_errors, 0);
    atomic_store(&g_order_consumed, 0);
    atomic_store(&g_order_stop, 0);
    
    pthread_t prod, cons;
    pthread_create(&prod, NULL, order_producer, NULL);
    pthread_create(&cons, NULL, order_consumer, NULL);
    
    pthread_join(prod, NULL);
    atomic_store(&g_order_stop, 1);
    pthread_join(cons, NULL);
    
    ASSERT_EQ(0, atomic_load(&g_order_errors));
    ASSERT_EQ(10000, atomic_load(&g_order_consumed));
    
    mpmc_destroy(&queue);
}
