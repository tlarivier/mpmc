/*
 * Folly MPMCQueue Adapter for Benchmarks
 * 
 */

#include "queue_interface.h"

#if defined(__linux__) && defined(FOLLY_ENABLED)

#include <folly/MPMCQueue.h>
#include <cstring>
#include <memory>

struct folly_ctx_t {
    std::unique_ptr<folly::MPMCQueue<uint64_t>> queue;
    uint32_t item_size;
};

static int folly_bench_init(void* ctx, uint32_t capacity, uint32_t item_size, uint32_t /*num_producers*/) {
    auto* c = static_cast<folly_ctx_t*>(ctx);
    c->item_size = item_size;
    c->queue = std::make_unique<folly::MPMCQueue<uint64_t>>(capacity);
    return 0;
}

static void folly_bench_destroy(void* ctx) {
    auto* c = static_cast<folly_ctx_t*>(ctx);
    c->queue.reset();
}

static int folly_bench_push(void* ctx, const void* item, uint32_t /*size*/, uint32_t /*producer_id*/) {
    auto* c = static_cast<folly_ctx_t*>(ctx);
    uint64_t val;
    std::memcpy(&val, item, sizeof(val));
    
    if (c->queue->write(val)) {
        return 0;
    }
    return -1;
}

static int folly_bench_pop(void* ctx, void* item, uint32_t /*size*/) {
    auto* c = static_cast<folly_ctx_t*>(ctx);
    uint64_t val;
    
    if (c->queue->read(val)) {
        std::memcpy(item, &val, sizeof(val));
        return 0;
    }
    return -1;
}

queue_ops_t queue_folly_ops = {
    "Folly",
    folly_bench_init,
    folly_bench_destroy,
    folly_bench_push,
    folly_bench_pop,
    sizeof(folly_ctx_t)
};

#else

// Stub for macOS - Folly has header conflicts
struct folly_stub_ctx_t {
    char dummy;
};

static int folly_stub_init(void*, uint32_t, uint32_t, uint32_t) { return -1; }
static void folly_stub_destroy(void*) {}
static int folly_stub_push(void*, const void*, uint32_t, uint32_t) { return -1; }
static int folly_stub_pop(void*, void*, uint32_t) { return -1; }

queue_ops_t queue_folly_ops = {
    "Folly (N/A)",
    folly_stub_init,
    folly_stub_destroy,
    folly_stub_push,
    folly_stub_pop,
    sizeof(folly_stub_ctx_t)
};

#endif
