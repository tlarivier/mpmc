/*
 * Moodycamel ConcurrentQueue Adapter for Benchmarks
 */

#include "concurrentqueue.h"
#include <cstring>

extern "C" {
#include "queue_interface.h"
}

struct moodycamel_ctx_t {
    moodycamel::ConcurrentQueue<uint64_t>* queue;
    uint32_t item_size;
    void* temp_buffer;
};

static int moodycamel_init(void* ctx, uint32_t capacity, uint32_t item_size, uint32_t num_producers) {
    auto* c = static_cast<moodycamel_ctx_t*>(ctx);
    (void)num_producers;
    c->item_size = item_size;
    c->queue = new moodycamel::ConcurrentQueue<uint64_t>(capacity);
    c->temp_buffer = malloc(item_size);
    return c->queue ? 0 : -1;
}

static void moodycamel_destroy(void* ctx) {
    auto* c = static_cast<moodycamel_ctx_t*>(ctx);
    delete c->queue;
    free(c->temp_buffer);
}

static int moodycamel_push(void* ctx, const void* item, uint32_t size, uint32_t producer_id) {
    auto* c = static_cast<moodycamel_ctx_t*>(ctx);
    (void)size;
    (void)producer_id;
    uint64_t val;
    std::memcpy(&val, item, sizeof(val));
    return c->queue->enqueue(val) ? 0 : -1;
}

static int moodycamel_pop(void* ctx, void* item, uint32_t size) {
    auto* c = static_cast<moodycamel_ctx_t*>(ctx);
    (void)size;
    uint64_t val;
    if (!c->queue->try_dequeue(val)) {
        return -1;
    }
    std::memcpy(item, &val, sizeof(val));
    return 0;
}

extern "C" queue_ops_t queue_moodycamel_ops = {
    .name     = "Moodycamel",
    .init     = moodycamel_init,
    .destroy  = moodycamel_destroy,
    .push     = moodycamel_push,
    .pop      = moodycamel_pop,
    .ctx_size = sizeof(moodycamel_ctx_t)
};
