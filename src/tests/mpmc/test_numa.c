/*
 * MPMC NUMA Tests
 */

#include "../test_common.h"
#include "mpmc_numa.h"

#ifdef __linux__
#include <numa.h>
#include <numaif.h>

/* ============================================================================
 * Basic NUMA Tests
 * ============================================================================ */

UTEST(mpmc_numa, init_destroy) {
    if (numa_available() < 0) {
        printf("NUMA not available, skipping\n");
        return;
    }
    
    mpmc_numa_t q;
    int ret = mpmc_numa_init(&q, TEST_SLOTS, TEST_SLOT_SIZE);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE(q.num_nodes > 0);
    ASSERT_TRUE(q.nodes != NULL);
    
    mpmc_numa_destroy(&q);
}

UTEST(mpmc_numa, single_node_ops) {
    if (numa_available() < 0) {
        printf("NUMA not available, skipping\n");
        return;
    }
    
    mpmc_numa_t q;
    ASSERT_EQ(0, mpmc_numa_init(&q, TEST_SLOTS, TEST_SLOT_SIZE));
    
    int node = numa_node_of_cpu(sched_getcpu());
    if (node < 0 || (uint32_t)node >= q.num_nodes) {
        node = 0;
    }
    
    /* Reserve and submit on local node */
    void *slot = mpmc_numa_reserve(&q, node, 0);
    ASSERT_TRUE(slot != NULL);
    
    *(uint64_t *)slot = 0xDEADBEEF;
    mpmc_numa_submit(&q, node, 0);
    
    /* Claim from local node */
    mpmc_item_t item = mpmc_numa_claim_local_first(&q, node);
    ASSERT_TRUE(item.data != NULL);
    ASSERT_EQ(0xDEADBEEF, *(uint64_t *)item.data);
    
    mpmc_numa_release(&q, &item);
    mpmc_numa_destroy(&q);
}

UTEST(mpmc_numa, local_first_preference) {
    if (numa_available() < 0) {
        printf("NUMA not available, skipping\n");
        return;
    }
    
    mpmc_numa_t q;
    ASSERT_EQ(0, mpmc_numa_init(&q, TEST_SLOTS, TEST_SLOT_SIZE));
    
    if (q.num_nodes < 2) {
        printf("Only 1 NUMA node, skipping multi-node test\n");
        mpmc_numa_destroy(&q);
        return;
    }
    
    /* Submit data on both node 0 and node 1 */
    void *slot0 = mpmc_numa_reserve(&q, 0, 0);
    ASSERT_TRUE(slot0 != NULL);
    *(uint64_t *)slot0 = 100;
    mpmc_numa_submit(&q, 0, 0);
    
    void *slot1 = mpmc_numa_reserve(&q, 1, 0);
    ASSERT_TRUE(slot1 != NULL);
    *(uint64_t *)slot1 = 200;
    mpmc_numa_submit(&q, 1, 0);
    
    /* Claim with local_first from node 0 - should get node 0's data first */
    mpmc_item_t item0 = mpmc_numa_claim_local_first(&q, 0);
    ASSERT_TRUE(item0.data != NULL);
    /* Should be from node 0 since we prefer local */
    ASSERT_EQ((uint32_t)0, item0.part >> 16);  /* High bits = node */
    
    mpmc_numa_release(&q, &item0);
    
    /* Claim remaining */
    mpmc_item_t item1 = mpmc_numa_claim_local_first(&q, 0);
    ASSERT_TRUE(item1.data != NULL);
    mpmc_numa_release(&q, &item1);
    
    mpmc_numa_destroy(&q);
}

/* ============================================================================
 * NUMA Concurrent Tests
 * ============================================================================ */

static mpmc_numa_t *g_numa_queue;
static _Atomic int g_numa_stop;
static _Atomic int g_numa_produced;
static _Atomic int g_numa_consumed;

static void *numa_producer(void *arg) {
    int node = (int)(uintptr_t)arg;
    for (int i = 0; i < 1000; i++) {
        void *slot;
        while ((slot = mpmc_numa_reserve(g_numa_queue, node, 0)) == NULL) {
            sched_yield();
        }
        *(int *)slot = i;
        mpmc_numa_submit(g_numa_queue, node, 0);
        atomic_fetch_add(&g_numa_produced, 1);
    }
    return NULL;
}

static void *numa_consumer(void *arg) {
    int node = (int)(uintptr_t)arg;
    while (!atomic_load(&g_numa_stop) || mpmc_numa_has_data(g_numa_queue)) {
        mpmc_item_t item = mpmc_numa_claim_local_first(g_numa_queue, node);
        if (item.data) {
            atomic_fetch_add(&g_numa_consumed, 1);
            mpmc_numa_release(g_numa_queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

UTEST(mpmc_numa, concurrent_multi_node) {
    if (numa_available() < 0) {
        printf("NUMA not available, skipping\n");
        return;
    }
    
    mpmc_numa_t queue;
    ASSERT_EQ(0, mpmc_numa_init(&queue, TEST_SLOTS, TEST_SLOT_SIZE));
    
    g_numa_queue = &queue;
    atomic_store(&g_numa_stop, 0);
    atomic_store(&g_numa_produced, 0);
    atomic_store(&g_numa_consumed, 0);
    
    int num_nodes = (queue.num_nodes > 2) ? 2 : queue.num_nodes;
    pthread_t prods[2], cons[2];
    
    for (int i = 0; i < num_nodes; i++) {
        pthread_create(&prods[i], NULL, numa_producer, (void *)(uintptr_t)i);
        pthread_create(&cons[i], NULL, numa_consumer, (void *)(uintptr_t)i);
    }
    
    for (int i = 0; i < num_nodes; i++) {
        pthread_join(prods[i], NULL);
    }
    atomic_store(&g_numa_stop, 1);
    for (int i = 0; i < num_nodes; i++) {
        pthread_join(cons[i], NULL);
    }
    
    ASSERT_EQ(num_nodes * 1000, atomic_load(&g_numa_produced));
    ASSERT_EQ(num_nodes * 1000, atomic_load(&g_numa_consumed));
    
    mpmc_numa_destroy(&queue);
}

#else /* !__linux__ */

UTEST(mpmc_numa, not_available) {
    printf("NUMA tests only available on Linux\n");
    ASSERT_TRUE(1);  /* Pass on non-Linux */
}

#endif /* __linux__ */
