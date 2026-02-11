/*
 * ConcurrencyKit Ring Buffer Adapter for Benchmarks
 */

#include "queue_interface.h"
#include <ck_ring.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

typedef struct {
    ck_ring_t ring;
    ck_ring_buffer_t* buffer;
    uint32_t capacity;
    uint32_t item_size;
    _Atomic uint64_t slot_counter;
} ck_ctx_t;

static int ck_bench_init(void* ctx, uint32_t capacity, uint32_t item_size, uint32_t num_producers) {
    ck_ctx_t* c = (ck_ctx_t*)ctx;
    (void)num_producers; 
    c->capacity  = capacity;
    c->item_size = item_size;
    
    c->buffer = malloc(sizeof(ck_ring_buffer_t) * capacity);
    if (!c->buffer) return -1;
    
    atomic_init(&c->slot_counter, 0);
    ck_ring_init(&c->ring, capacity);
    return 0;
}

static void ck_bench_destroy(void* ctx) {
    ck_ctx_t* c = (ck_ctx_t*)ctx;
    free(c->buffer);
}

static int ck_bench_push(void* ctx, const void* item, uint32_t size, uint32_t producer_id) {
    ck_ctx_t* c = (ck_ctx_t*)ctx;
    (void)size;
    (void)producer_id;
    
    uint64_t val = *(const uint64_t*)item;
    return ck_ring_enqueue_mpmc(&c->ring, c->buffer, (void*)val) ? 0 : -1;
}

static int ck_bench_pop(void* ctx, void* item, uint32_t size) {
    ck_ctx_t* c = (ck_ctx_t*)ctx;
    (void)size;
    
    void* ptr;
    if (!ck_ring_dequeue_mpmc(&c->ring, c->buffer, &ptr)) {
        return -1;
    }
    *(uint64_t*)item = (uint64_t)ptr;
    return 0;
}

queue_ops_t queue_ck_ops = {
    .name     = "CK",
    .init     = ck_bench_init,
    .destroy  = ck_bench_destroy,
    .push     = ck_bench_push,
    .pop      = ck_bench_pop,
    .ctx_size = sizeof(ck_ctx_t)
};
