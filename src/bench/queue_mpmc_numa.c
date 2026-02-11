/*
 * MPMC NUMA Queue Adapter for Benchmarks
 * 
 */

#include "queue_interface.h"
#include "mpmc_numa.h"
#include <string.h>

typedef struct {
    mpmc_numa_t queue;
    uint32_t item_size;
    int num_nodes;
    uint32_t partitions_per_node;
} mpmc_numa_ctx_t;

static int mpmc_numa_bench_init(void* ctx, uint32_t capacity, uint32_t item_size, uint32_t num_producers) {
    mpmc_numa_ctx_t* c = (mpmc_numa_ctx_t*)ctx;
    c->item_size = item_size;
    
    c->num_nodes = mpmc_numa_get_node_count();
    if (c->num_nodes > MPMC_NUMA_MAX_NODES) {
        c->num_nodes = MPMC_NUMA_MAX_NODES;
    }
    
    c->partitions_per_node = (num_producers + c->num_nodes - 1) / c->num_nodes;
    if (c->partitions_per_node == 0) c->partitions_per_node = 1;
    
    mpmc_numa_config_t config = {
        .num_nodes           = c->num_nodes,
        .partitions_per_node = c->partitions_per_node,
        .slots_per_partition = capacity,
        .slot_size           = item_size
    };
    
    return mpmc_numa_init(&c->queue, &config);
}

static void mpmc_numa_bench_destroy(void* ctx) {
    mpmc_numa_ctx_t* c = (mpmc_numa_ctx_t*)ctx;
    mpmc_numa_destroy(&c->queue);
}

static int mpmc_numa_bench_push(void* ctx, const void* item, uint32_t size, uint32_t producer_id) {
    mpmc_numa_ctx_t* c = (mpmc_numa_ctx_t*)ctx;
    
    int node = (producer_id / c->partitions_per_node) % c->num_nodes;
    uint32_t local_part = producer_id % c->partitions_per_node;
    
    void* slot = mpmc_numa_reserve(&c->queue, node, local_part);
    if (!slot) return -1;
    memcpy(slot, item, size);
    mpmc_numa_submit(&c->queue, node, local_part);
    return 0;
}

static int mpmc_numa_bench_pop(void* ctx, void* item, uint32_t size) {
    mpmc_numa_ctx_t* c = (mpmc_numa_ctx_t*)ctx;
    
    mpmc_numa_item_t it = mpmc_numa_claim_local_first(&c->queue);
    if (!it.data) return -1;
    memcpy(item, it.data, size);
    mpmc_numa_release(&c->queue, &it);
    return 0;
}

queue_ops_t queue_mpmc_numa_ops = {
    .name     = "MPMC-NUMA",
    .init     = mpmc_numa_bench_init,
    .destroy  = mpmc_numa_bench_destroy,
    .push     = mpmc_numa_bench_push,
    .pop      = mpmc_numa_bench_pop,
    .ctx_size = sizeof(mpmc_numa_ctx_t)
};
