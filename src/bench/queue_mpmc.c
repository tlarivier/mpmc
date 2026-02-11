/*
 * MPMC Queue Adapter for Benchmarks
 * 
 * Uses N partitions (1 per producer) - the core advantage of MPMC design.
 */

#include "queue_interface.h"
#include "mpmc.h"
#include <string.h>

typedef struct {
    mpmc_t queue;
    uint32_t item_size;
    uint32_t num_partitions;
} mpmc_ctx_t;

static int mpmc_bench_init(void* ctx, uint32_t capacity, uint32_t item_size, uint32_t num_producers) {
    mpmc_ctx_t* c = (mpmc_ctx_t*)ctx;
    c->item_size      = item_size;
    c->num_partitions = num_producers > 0 ? num_producers : 1;
    return mpmc_init(&c->queue, c->num_partitions, capacity, item_size);
}

static void mpmc_bench_destroy(void* ctx) {
    mpmc_ctx_t* c = (mpmc_ctx_t*)ctx;
    mpmc_destroy(&c->queue);
}

static int mpmc_bench_push(void* ctx, const void* item, uint32_t size, uint32_t producer_id) {
    mpmc_ctx_t* c = (mpmc_ctx_t*)ctx;
    uint32_t partition = producer_id % c->num_partitions;
    void* slot = mpmc_reserve(&c->queue, partition);
    if (!slot) return -1;
    memcpy(slot, item, size);
    mpmc_submit(&c->queue, partition);
    return 0;
}

static int mpmc_bench_pop(void* ctx, void* item, uint32_t size) {
    mpmc_ctx_t* c = (mpmc_ctx_t*)ctx;
    mpmc_item_t it = mpmc_claim(&c->queue);
    if (!it.data) return -1;
    memcpy(item, it.data, size);
    mpmc_release(&c->queue, &it);
    return 0;
}

queue_ops_t queue_mpmc_ops = {
    .name     = "MPMC",
    .init     = mpmc_bench_init,
    .destroy  = mpmc_bench_destroy,
    .push     = mpmc_bench_push,
    .pop      = mpmc_bench_pop,
    .ctx_size = sizeof(mpmc_ctx_t)
};
