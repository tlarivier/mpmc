/*
 * MPMC-RR - Round-Robin Multi-Producer Multi-Consumer Queue
 *
 * Lock-free bounded queue with partitioned design.
 * Each partition is intended for a single producer (no contention on reserve).
 * Multiple consumers can claim from any partition via sequential scanning.
 * Note: Consumers always scan from partition 0, slot 0 - no round-robin hints.
 *
 * Guarantees:
 *   - Lock-free bounded: Operations complete in O(P×S) steps
 *   - NOT obstruction-free: Slots stuck in FILLING block progress
 *   - NOT wait-free: Operations may fail if queue is full/empty
 *
 * Limitation: If a producer crashes after reserve() but before submit(),
 * the slot remains in FILLING state forever. No recovery mechanism exists.
 *
 * Memory Model:
 *   - Producer: reserve(acquire) -> write data -> submit(release)
 *   - Consumer: claim(acquire) -> read data -> release(release)
 *   - Synchronization chain:
 *       release(mpmc_release) --sync--> acquire(mpmc_reserve)  [slot reuse]
 *       release(mpmc_submit)  --sync--> acquire(mpmc_claim)    [data transfer]
 *
 * Usage Contract:
 *   - Each partition p should be used by exactly ONE producer thread
 *   - Multiple consumers can call claim() concurrently
 *   - Violating single-producer-per-partition causes undefined behavior
 *
 * WARNING: head/current fields are NOT atomic. Multiple producers on
 * the same partition will cause data races and undefined behavior.
 * This is by design for zero-contention producer path.
 *
 * NOTE: This header uses _Thread_local for consumer rotation. If included
 * in multiple translation units, each TU gets its own TLS variable, which
 * is the intended behavior (per-thread rotation state).
 */

#ifndef MPMC_H
#define MPMC_H

#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#define MPMC_CACHE_LINE 64
#define MPMC_EMPTY   0
#define MPMC_FILLING 1
#define MPMC_READY   2
#define MPMC_READING 3

/*
 * Slot state: 2 bits state + 62 bits generation counter
 * Padded to cache line to avoid false sharing between adjacent slots.
 * Trade-off: Uses more memory but eliminates contention on CAS.
 */
typedef struct {
    _Atomic uint64_t state;
    uint8_t _pad[MPMC_CACHE_LINE - sizeof(uint64_t)];
} mpmc_slot_t;

typedef struct {
    mpmc_slot_t *slots;
    uint8_t *data;
    uint32_t num_slots;
    uint32_t slot_size;
    uint32_t mask;
    uint32_t head;
    uint32_t current;
#ifndef NDEBUG
    _Atomic uint64_t owner_tid;  /* Debug: detect single-producer violation */
#endif
    uint8_t _pad1[MPMC_CACHE_LINE - 36];
} __attribute__((aligned(MPMC_CACHE_LINE))) mpmc_part_t;

typedef struct {
    mpmc_part_t *parts;
    uint32_t num_parts;
    uint32_t slots_per_part;
    uint32_t slot_size;
    uint32_t part_mask;  /* num_parts - 1 for fast modulo */
    uint8_t _pad1[MPMC_CACHE_LINE - 20];
    
    void *mem;
} mpmc_t;

typedef struct {
    void *data;
    uint32_t part;
    uint32_t slot;
} mpmc_item_t;

/* ============================================================================
 * Init / Destroy
 * ============================================================================ */

static inline int mpmc_init(mpmc_t *m, uint32_t num_parts, uint32_t slots, uint32_t slot_size) {
    if (num_parts == 0 || (num_parts & (num_parts - 1)) != 0) return -1;  /* Must be power-of-2 */
    if (slots     == 0 || (slots & (slots - 1)) != 0) return -1;
    if (slot_size == 0) return -1;
    
    m->num_parts      = num_parts;
    m->slots_per_part = slots;
    m->slot_size      = slot_size;
    m->part_mask      = num_parts - 1;
    
    /* Overflow-safe size calculations */
    size_t parts_size, slots_total, slots_size, data_size, total;
    if (__builtin_mul_overflow((size_t)num_parts, sizeof(mpmc_part_t), &parts_size)) return -1;
    if (__builtin_mul_overflow((size_t)num_parts, (size_t)slots, &slots_total)) return -1;
    if (__builtin_mul_overflow(slots_total, sizeof(mpmc_slot_t), &slots_size)) return -1;
    if (__builtin_mul_overflow(slots_total, (size_t)slot_size, &data_size)) return -1;
    if (__builtin_add_overflow(parts_size, slots_size, &total)) return -1;
    if (__builtin_add_overflow(total, data_size, &total)) return -1;
    if (__builtin_add_overflow(total, (size_t)MPMC_CACHE_LINE, &total)) return -1;
    
    total = (total + MPMC_CACHE_LINE - 1) & ~(size_t)(MPMC_CACHE_LINE - 1);
    
    m->mem = aligned_alloc(MPMC_CACHE_LINE, total);
    if (!m->mem) return -1;
    memset(m->mem, 0, total);
    
    uint8_t *ptr = (uint8_t *)m->mem;
    m->parts = (mpmc_part_t *)ptr; ptr += parts_size;
    mpmc_slot_t *all_slots = (mpmc_slot_t *)ptr; ptr += slots_size;
    uint8_t *all_data = ptr;
    
    for (uint32_t p = 0; p < num_parts; p++) {
        m->parts[p].slots     = &all_slots[p * slots];
        m->parts[p].data      = &all_data[p * slots * slot_size];
        m->parts[p].num_slots = slots;
        m->parts[p].slot_size = slot_size;
        m->parts[p].mask      = slots - 1;
        m->parts[p].head      = 0;
        m->parts[p].current   = UINT32_MAX;
#ifndef NDEBUG
        atomic_store(&m->parts[p].owner_tid, 0);
#endif
    }
    
    return 0;
}

static inline void mpmc_destroy(mpmc_t *m) {
    free(m->mem);
    m->mem            = NULL;
    m->parts          = NULL;
    m->num_parts      = 0;
    m->slots_per_part = 0;
    m->slot_size      = 0;
}

/* ============================================================================
 * Producer: reserve / submit 
 * ============================================================================ */

static inline void *mpmc_reserve(mpmc_t *m, uint32_t p) {
    if (p >= m->num_parts) return NULL;
    mpmc_part_t *part = &m->parts[p];
    
#ifndef NDEBUG
    /* Detect single-producer contract violation */
    uint64_t tid = (uint64_t)pthread_self();  /* Reliable thread ID */
    uint64_t expected = 0;
    if (!atomic_compare_exchange_strong(&part->owner_tid, &expected, tid)) {
        assert(expected == tid && "SINGLE-PRODUCER VIOLATION: partition used by multiple threads");
    }
#endif
    
    if (part->current != UINT32_MAX) return NULL;
    
    for (uint32_t i = 0; i < part->num_slots; i++) {
        uint32_t idx = (part->head + i) & part->mask;
        mpmc_slot_t *slot = &part->slots[idx];
        
        uint64_t cur = atomic_load_explicit(&slot->state, memory_order_relaxed);  /* relaxed OK, CAS has acquire */
        if ((cur & 3) != MPMC_EMPTY) continue;
        
        uint64_t next = (cur & ~3ULL) | MPMC_FILLING;
        /* acquire synchronizes with release in mpmc_release */
        if (atomic_compare_exchange_strong_explicit(&slot->state, &cur, next,
                memory_order_acquire, memory_order_relaxed)) {
            part->head = (idx + 1) & part->mask;
            part->current = idx;
            return &part->data[idx * part->slot_size];
        }
    }
    
    return NULL;
}

static inline void mpmc_submit(mpmc_t *m, uint32_t p) {
    if (p >= m->num_parts) return;
    mpmc_part_t *part = &m->parts[p];
    
    if (part->current == UINT32_MAX) return;
    
    mpmc_slot_t *slot = &part->slots[part->current];
    uint64_t cur  = atomic_load_explicit(&slot->state, memory_order_relaxed);
    assert((cur & 3) == MPMC_FILLING && "submit called on non-FILLING slot");
    uint64_t next = ((cur >> 2) + 1) << 2 | MPMC_READY;
    atomic_store_explicit(&slot->state, next, memory_order_release);
    
    part->current = UINT32_MAX;
}

static inline void mpmc_discard(mpmc_t *m, uint32_t p) {
    if (p >= m->num_parts) return;
    mpmc_part_t *part = &m->parts[p];
    
    if (part->current == UINT32_MAX) return;
    
    mpmc_slot_t *slot = &part->slots[part->current];
    uint64_t cur  = atomic_load_explicit(&slot->state, memory_order_relaxed);
    assert((cur & 3) == MPMC_FILLING && "discard called on non-FILLING slot");
    uint64_t next = ((cur >> 2) + 1) << 2 | MPMC_EMPTY;
    atomic_store_explicit(&slot->state, next, memory_order_release);  /* release for safety */
    
    part->current = UINT32_MAX;
}

/* ============================================================================
 * Consumer: claim / release 
 * ============================================================================ */

static inline mpmc_item_t mpmc_claim(mpmc_t *m) {
    mpmc_item_t item = {NULL, UINT32_MAX, UINT32_MAX};
    static _Thread_local uint32_t tl_start_p = 0;  /* Thread-local rotation to avoid starvation */
    
    uint32_t start_p = tl_start_p;
    
    /* Scan all partitions and slots with rotation */
    for (uint32_t pi = 0; pi < m->num_parts; pi++) {
        uint32_t p = (start_p + pi) & m->part_mask;
        mpmc_part_t *part = &m->parts[p];
        
        for (uint32_t s = 0; s < part->num_slots; s++) {
            mpmc_slot_t *slot = &part->slots[s];
            
            uint64_t cur = atomic_load_explicit(&slot->state, memory_order_relaxed);  /* relaxed OK, CAS has acquire */
            if ((cur & 3) != MPMC_READY) continue;
            
            uint64_t next = (cur & ~3ULL) | MPMC_READING;
            if (atomic_compare_exchange_strong_explicit(&slot->state, &cur, next,
                    memory_order_acquire, memory_order_relaxed)) {
                tl_start_p = (p + 1) & m->part_mask;  /* Rotate for next call */
                item.data = &part->data[s * part->slot_size];
                item.part = p;
                item.slot = s;
                return item;
            }
        }
    }
    
    tl_start_p = (start_p + 1) & m->part_mask;  /* Rotate even on failure */
    return item;
}

static inline void mpmc_release(mpmc_t *m, mpmc_item_t *item) {
    if (item->part == UINT32_MAX) return;
    if (item->part >= m->num_parts) return;
    
    mpmc_part_t *part = &m->parts[item->part];
    if (item->slot >= part->num_slots) return;
    mpmc_slot_t *slot = &part->slots[item->slot];
    
    uint64_t cur = atomic_load_explicit(&slot->state, memory_order_relaxed);
    if ((cur & 3) != MPMC_READING) {
        /* Double-release or corrupted state - ignore silently to prevent corruption */
        assert(0 && "release called on non-READING slot (double-release?)");
        item->data = NULL;
        item->part = UINT32_MAX;
        return;
    }
    
    uint64_t next = ((cur >> 2) + 1) << 2 | MPMC_EMPTY;
    atomic_store_explicit(&slot->state, next, memory_order_release);
    
    item->data = NULL;
    item->part = UINT32_MAX;
}

/* ============================================================================
 * Batch API
 * ============================================================================ */

#define MPMC_MAX_BATCH 64

typedef struct {
    mpmc_item_t items[MPMC_MAX_BATCH];
    uint32_t count;
} mpmc_batch_t;

/*
 * True batch claim: single scan, collects up to max READY slots.
 * Complexity: O(P×S) regardless of how many items found.
 * Much more efficient than repeated mpmc_claim() calls.
 */
static inline int mpmc_claim_batch(mpmc_t *m, mpmc_batch_t *batch, uint32_t max) {
    batch->count = 0;
    if (max > MPMC_MAX_BATCH) max = MPMC_MAX_BATCH;
    if (max == 0) return 0;
    
    static _Thread_local uint32_t tl_start_p = 0;  /* Thread-local rotation */
    uint32_t start_p = tl_start_p;
    
    /* Scan all partitions and slots with rotation */
    for (uint32_t pi = 0; pi < m->num_parts && batch->count < max; pi++) {
        uint32_t p = (start_p + pi) & m->part_mask;
        mpmc_part_t *part = &m->parts[p];
        
        for (uint32_t s = 0; s < part->num_slots && batch->count < max; s++) {
            mpmc_slot_t *slot = &part->slots[s];
            
            uint64_t cur = atomic_load_explicit(&slot->state, memory_order_relaxed);  /* relaxed OK, CAS has acquire */
            if ((cur & 3) != MPMC_READY) continue;
            
            uint64_t next = (cur & ~3ULL) | MPMC_READING;
            if (atomic_compare_exchange_strong_explicit(&slot->state, &cur, next,
                    memory_order_acquire, memory_order_relaxed)) {
                batch->items[batch->count].data = &part->data[s * part->slot_size];
                batch->items[batch->count].part = p;
                batch->items[batch->count].slot = s;
                batch->count++;
            }
        }
    }
    
    tl_start_p = (start_p + 1) & m->part_mask;  /* Rotate for next call */
    return (int)batch->count;
}

static inline void mpmc_release_batch(mpmc_t *m, mpmc_batch_t *batch) {
    for (uint32_t i = 0; i < batch->count; i++) {
        mpmc_release(m, &batch->items[i]);
    }
    batch->count = 0;
}

/* ============================================================================
 * Utilities
 * ============================================================================ */

/*
 * DEPRECATED: Non-linearizable approximate check. Use mpmc_claim() instead.
 * Only use for termination conditions with timeout fallback.
 */
static inline int mpmc_has_data(const mpmc_t *m) {
    for (uint32_t p = 0; p < m->num_parts; p++) {
        for (uint32_t s = 0; s < m->parts[p].num_slots; s++) {
            if ((atomic_load_explicit(&m->parts[p].slots[s].state, memory_order_relaxed) & 3) == MPMC_READY)
                return 1;
        }
    }
    return 0;
}

static inline mpmc_item_t mpmc_claim_partition(mpmc_t *m, uint32_t p) {
    mpmc_item_t item = {NULL, UINT32_MAX, UINT32_MAX};
    if (p >= m->num_parts) return item;
    
    mpmc_part_t *part = &m->parts[p];
    
    for (uint32_t s = 0; s < part->num_slots; s++) {
        mpmc_slot_t *slot = &part->slots[s];
        
        uint64_t cur = atomic_load_explicit(&slot->state, memory_order_relaxed);  /* relaxed OK, CAS has acquire */
        if ((cur & 3) != MPMC_READY) continue;
        
        uint64_t next = (cur & ~3ULL) | MPMC_READING;
        if (atomic_compare_exchange_strong_explicit(&slot->state, &cur, next,
                memory_order_acquire, memory_order_relaxed)) {
            item.data = &part->data[s * part->slot_size];
            item.part = p;
            item.slot = s;
            return item;
        }
    }
    return item;
}

#endif /* MPMC_H */
