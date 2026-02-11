/*
 * Vyukov/Rigtorp MPMCQueue Adapter for Benchmarks
 */

#include "rigtorp/MPMCQueue.h"
#include <cstring>

extern "C" {
#include "queue_interface.h"
}

struct vyukov_ctx_t {
    rigtorp::MPMCQueue<uint64_t>* queue;
    uint32_t item_size;
};

static int vyukov_init(void* ctx, uint32_t capacity, uint32_t item_size, uint32_t num_producers) {
    auto* c = static_cast<vyukov_ctx_t*>(ctx);
    (void)num_producers;
    c->item_size = item_size;
    c->queue = new rigtorp::MPMCQueue<uint64_t>(capacity);
    return c->queue ? 0 : -1;
}

static void vyukov_destroy(void* ctx) {
    auto* c = static_cast<vyukov_ctx_t*>(ctx);
    delete c->queue;
}

static int vyukov_push(void* ctx, const void* item, uint32_t size, uint32_t producer_id) {
    auto* c = static_cast<vyukov_ctx_t*>(ctx);
    (void)size;
    (void)producer_id;
    uint64_t val;
    std::memcpy(&val, item, sizeof(val));
    return c->queue->try_push(val) ? 0 : -1;
}

static int vyukov_pop(void* ctx, void* item, uint32_t size) {
    auto* c = static_cast<vyukov_ctx_t*>(ctx);
    (void)size;
    uint64_t val;
    if (!c->queue->try_pop(val)) {
        return -1;
    }
    std::memcpy(item, &val, sizeof(val));
    return 0;
}

extern "C" queue_ops_t queue_vyukov_ops = {
    .name     = "Vyukov",
    .init     = vyukov_init,
    .destroy  = vyukov_destroy,
    .push     = vyukov_push,
    .pop      = vyukov_pop,
    .ctx_size = sizeof(vyukov_ctx_t)
};
