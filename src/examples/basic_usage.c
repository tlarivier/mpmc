/*
 * MPMC Basic Usage Example
 */

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "mpmc.h"

#define NUM_MESSAGES 100

typedef struct {
    uint64_t id;
    char data[56];
} message_t;

static mpmc_t queue;

void* producer(void* arg) {
    uint32_t partition = (uint32_t)(uintptr_t)arg;
    
    for (int i = 0; i < NUM_MESSAGES; i++) {
        message_t* slot = (message_t*)mpmc_reserve(&queue, partition);
        if (slot) {
            slot->id = partition * 1000 + i;
            snprintf(slot->data, sizeof(slot->data), "Message %d from P%u", i, partition);
            mpmc_submit(&queue, partition);
        }
    }
    
    printf("Producer %u: sent %d messages\n", partition, NUM_MESSAGES);
    return NULL;
}

void* consumer(void* arg) {
    (void)arg;
    int count = 0;
    int empty_rounds = 0;
    
    while (empty_rounds < 1000) {
        mpmc_item_t item = mpmc_claim(&queue);
        if (item.data) {
            message_t* msg = (message_t*)item.data;
            if (count < 5) {
                printf("Consumer: got id=%llu data='%s'\n", 
                       (unsigned long long)msg->id, msg->data);
            }
            mpmc_release(&queue, &item);
            count++;
            empty_rounds = 0;
        } else {
            empty_rounds++;
        }
    }
    
    printf("Consumer: received %d messages\n", count);
    return NULL;
}

int main(void) {
    printf("=== MPMC Basic Usage Example ===\n\n");
    
    /* Initialize queue: 4 partitions, 64 slots each, 64 bytes/slot */
    if (mpmc_init(&queue, 4, 64, sizeof(message_t)) != 0) {
        fprintf(stderr, "Failed to initialize queue\n");
        return 1;
    }
    
    /* Start producers first, then consumer */
    pthread_t producers[4], consumer_thread;
    
    for (int i = 0; i < 4; i++) {
        pthread_create(&producers[i], NULL, producer, (void*)(uintptr_t)i);
    }
    pthread_create(&consumer_thread, NULL, consumer, NULL);
    
    /* Wait for completion */
    for (int i = 0; i < 4; i++) {
        pthread_join(producers[i], NULL);
    }
    pthread_join(consumer_thread, NULL);
    
    mpmc_destroy(&queue);
    printf("\nDone.\n");
    return 0;
}
