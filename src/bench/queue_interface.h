/*
 * Queue Benchmark Interface
 * 
 * Common interface for benchmarking different queue implementations.
 */

#ifndef QUEUE_INTERFACE_H
#define QUEUE_INTERFACE_H

#include <stdint.h>
#include <stddef.h>

typedef struct {
    const char* name;
    int  (*init)(void* ctx, uint32_t capacity, uint32_t item_size, uint32_t num_producers);
    void (*destroy)(void* ctx);
    int  (*push)(void* ctx, const void* item, uint32_t size, uint32_t producer_id);
    int  (*pop)(void* ctx, void* item, uint32_t size);
    size_t ctx_size;
} queue_ops_t;

extern queue_ops_t queue_mpmc_ops;
extern queue_ops_t queue_mpmc_numa_ops;
extern queue_ops_t queue_ck_ops;
extern queue_ops_t queue_moodycamel_ops;
extern queue_ops_t queue_vyukov_ops;
extern queue_ops_t queue_folly_ops;

#endif /* QUEUE_INTERFACE_H */
