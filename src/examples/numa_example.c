/*
 * MPMC NUMA Example
 * Build (Linux with libnuma):
 *   gcc -O3 -o numa_example numa_example.c -lnuma -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>

#include "mpmc_numa.h"

#define MESSAGES_PER_PRODUCER 10000
#define PARTITIONS_PER_NODE   2

typedef struct {
    uint64_t producer_id;
    uint64_t seq;
    uint64_t timestamp;
    int source_node;
} message_t;

static mpmc_numa_t queue;
static atomic_int producers_done = 0;

/* Producer thread - writes to its NUMA-local partition */
void* producer_thread(void* arg) {
    int producer_id = (int)(uintptr_t)arg;
    int my_node = mpmc_numa_get_current_node();
    if (my_node >= queue.num_nodes) my_node = 0;
    
    /* Calculate local partition within this node */
    uint32_t local_part = producer_id % PARTITIONS_PER_NODE;
    
    printf("Producer %d on node %d, local_part %u\n", 
           producer_id, my_node, local_part);
    
    for (int i = 0; i < MESSAGES_PER_PRODUCER; i++) {
        message_t* slot;
        
        /* Spin until slot available */
        while ((slot = (message_t*)mpmc_numa_reserve(&queue, my_node, local_part)) == NULL) {
            sched_yield();
        }
        
        slot->producer_id = producer_id;
        slot->seq         = i;
        slot->timestamp   = (uint64_t)i;
        slot->source_node = my_node;
        
        mpmc_numa_submit(&queue, my_node, local_part);
    }
    
    atomic_fetch_add(&producers_done, 1);
    return NULL;
}

/* Consumer thread - uses local-first scanning */
void* consumer_thread(void* arg) {
    int consumer_id = (int)(uintptr_t)arg;
    int my_node = mpmc_numa_get_current_node();
    if (my_node >= queue.num_nodes) my_node = 0;
    
    printf("Consumer %d on node %d (local-first)\n", consumer_id, my_node);
    
    int local_count  = 0;
    int remote_count = 0;
    int total        = 0;
    
    int expected_producers = queue.num_nodes * PARTITIONS_PER_NODE;
    
    while (atomic_load(&producers_done) < expected_producers || total < 1) {
        mpmc_numa_item_t item = mpmc_numa_claim_local_first(&queue);
        
        if (item.data) {
            message_t* msg = (message_t*)item.data;
            
            if (msg->source_node == my_node) {
                local_count++;
            } else {
                remote_count++;
            }
            total++;
            
            mpmc_numa_release(&queue, &item);
        } else {
            sched_yield();
        }
    }
    
    /* Drain remaining */
    int drain_attempts = 0;
    while (drain_attempts < 1000) {
        mpmc_numa_item_t item = mpmc_numa_claim_local_first(&queue);
        if (item.data) {
            message_t* msg = (message_t*)item.data;
            if (msg->source_node == my_node) local_count++;
            else remote_count++;
            total++;
            mpmc_numa_release(&queue, &item);
            drain_attempts = 0;
        } else {
            drain_attempts++;
            sched_yield();
        }
    }
    
    printf("Consumer %d: total=%d local=%d (%.1f%%) remote=%d\n",
           consumer_id, total, local_count, 
           total > 0 ? 100.0 * local_count / total : 0,
           remote_count);
    
    return NULL;
}

int main(void) {
    printf("=== MPMC NUMA Example ===\n\n");
    
    /* Detect NUMA topology */
    int num_nodes = mpmc_numa_get_node_count();
    printf("Detected %d NUMA node(s)\n\n", num_nodes);
    
    if (num_nodes < 2) {
        printf("Note: Single NUMA node - running in compatibility mode.\n");
        printf("      For real NUMA benefits, run on multi-socket system.\n\n");
    }
    
    /* Configure queue */
    mpmc_numa_config_t config = {
        .num_nodes           = num_nodes,
        .partitions_per_node = PARTITIONS_PER_NODE,
        .slots_per_partition = 256,
        .slot_size           = sizeof(message_t)
    };
    
    if (mpmc_numa_init(&queue, &config) != 0) {
        fprintf(stderr, "Failed to initialize NUMA queue\n");
        return 1;
    }
    
    mpmc_numa_print_info(&queue);
    printf("\n");
    
    int num_producers = num_nodes * PARTITIONS_PER_NODE;
    int num_consumers = num_nodes;
    
    pthread_t producers[num_producers];
    pthread_t consumers[num_consumers];
    
    printf("Starting %d producers, %d consumers\n\n", 
           num_producers, num_consumers);
    
    for (int i = 0; i < num_producers; i++) {
        pthread_create(&producers[i], NULL, producer_thread, (void*)(uintptr_t)i);
    }
    
    usleep(1000);
    
    for (int i = 0; i < num_consumers; i++) {
        pthread_create(&consumers[i], NULL, consumer_thread, (void*)(uintptr_t)i);
    }
    
    for (int i = 0; i < num_producers; i++) {
        pthread_join(producers[i], NULL);
    }
    for (int i = 0; i < num_consumers; i++) {
        pthread_join(consumers[i], NULL);
    }
    
    printf("\n");
    mpmc_numa_destroy(&queue);
    printf("Done.\n");
    
    return 0;
}
