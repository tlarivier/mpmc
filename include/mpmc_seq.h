/*
 * mpmc_seq.h — partitioned bounded MPMC queue: produce_seq + consume_seq
 *              per partition, range claims, gated backpressure via rpool.
 *
 * Three components, all zero-allocation (caller provides aligned buffers):
 *   mseq_t        — array of partitions; each partition is SPMC.
 *   mseq_rpool_t  — per-consumer cursor (1 cache line each), for gated
 *                   backpressure: producer scans cursors to find the
 *                   oldest in-flight read on its partition.
 *   mseq_claim_t  — handle for a contiguous (logical) range of items;
 *                   the physical range may wrap the ring buffer.
 *
 * Cost per item (gated mode):
 *   Producer : 1 store-release (produce_seq) + O(C) Shared loads (rpool)
 *   Consumer : 1 CAS / N items (consume_seq) + 2 own-line stores (set/clear)
 *
 * Invariants:
 *  - sizeof(mseq_part_t)   == 2 * MSEQ_CACHE_LINE (compile-time assert).
 *  - sizeof(mseq_reader_t) == 1 * MSEQ_CACHE_LINE (compile-time assert).
 *  - num_parts and slots are powers of 2; buffer aligned to MSEQ_CACHE_LINE.
 *  - One producer per partition. The "reserved" flag is a violation
 *    detector via atomic_exchange — it does NOT provide ordering for
 *    cross-thread handoff. Same-thread reserve/submit is the contract.
 *  - Pre-publish rp->readers[cid].part (release) BEFORE the CAS on
 *    consume_seq — reversing this order opens a wrap race in which the
 *    producer can overwrite a slot a consumer is about to claim.
 *  - Iteration over a claim must go through mseq_claim_item(); claims
 *    can wrap the ring and the underlying data is not contiguous past
 *    the boundary.
 *
 * Not allowed:
 *  - Touch consume_seq from mseq_release (the CAS in claim already
 *    advanced it; any further store there breaks backpressure).
 *  - Mix gated and ungated producers on the same queue (an ungated
 *    producer ignores rpool cursors and can overwrite live readers).
 *  - Reuse a cid across two concurrent consumers (corruption silent —
 *    no runtime check).
 *  - Hand off a partition's producer slot mid-reserve without external
 *    synchronization. The reserved-bit detects races, not synchronizes.
 *  - Iterate a wrapped claim with claim.data + i*slot_size — past the
 *    ring end the pointer goes off-buffer. Use mseq_claim_item().
 */

#ifndef MPMC_SEQ_H
#define MPMC_SEQ_H

#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <sched.h>

#ifdef MSEQ_DEBUG
#include <stdio.h>
#include <pthread.h>
#endif

#ifndef MSEQ_CACHE_LINE
#define MSEQ_CACHE_LINE 128
#endif
#define MSEQ_IDLE UINT32_MAX

/* ============================================================================
 * Queue structures
 * ============================================================================ */

typedef struct {
    /* Line 0: PRODUCER */
    uint8_t *data;
    uint32_t num_slots;
    uint32_t slot_size;
    uint32_t mask;
    _Atomic uint32_t reserved;
    _Atomic uint64_t produce_seq;
    uint8_t _p0[MSEQ_CACHE_LINE - 32];

    /* Line 1: CONSUMER CLAIM */
    _Atomic uint64_t consume_seq;
    uint8_t _p1[MSEQ_CACHE_LINE - 8];
} __attribute__((aligned(MSEQ_CACHE_LINE))) mseq_part_t;

_Static_assert(sizeof(mseq_part_t) == 2 * MSEQ_CACHE_LINE,
               "mseq_part_t size must be exactly 2 cache lines");

typedef struct {
    mseq_part_t *parts;
    uint32_t num_parts;
    uint32_t slots_per_part;
    uint32_t slot_size;
    uint32_t part_mask;
} mseq_t;

typedef struct {
    void    *data;      /* pointer to first item (physical slot start & mask) */
    uint32_t part;
    uint32_t count;
    uint64_t start;     /* logical start position (for release) */
    uint32_t mask;      /* slot index mask (slots_per_part - 1) for wrap iteration */
    uint64_t start_idx; /* logical start index for wrap-safe iteration:
                           slot_i = base[((start_idx + i) & mask) * slot_size] */
} mseq_claim_t;

/*
 * Access item `i` within a claim. Handles ring wrap-around correctly.
 *
 * IMPORTANT: when a range claim wraps around the ring boundary (e.g.,
 * slots [6,7,0,1] in an 8-slot ring), the items are NOT contiguous in
 * memory. `claim.data` points to the first item, but `claim.data + i`
 * is WRONG for items past the ring end. Always use this function:
 *
 *   for (uint32_t i = 0; i < claim.count; i++) {
 *       my_msg_t *msg = mseq_claim_item(q, &claim, i);
 *       process(msg);
 *   }
 */
static inline void *mseq_claim_item(const mseq_t *q, const mseq_claim_t *c,
                                     uint32_t index) {
    if (index >= c->count) return NULL;
    if (c->part >= q->num_parts) return NULL;
    uint32_t slot = (c->start_idx + index) & c->mask;
    return &q->parts[c->part].data[(size_t)slot * q->slot_size];
}

/* ============================================================================
 * Reader pool: per-consumer published cursor (gated backpressure)
 *
 * Each reader has its own cache line. Written ONLY by its owner.
 * The producer reads all readers (Shared loads, free) to find the
 * minimum outstanding claim.
 * ============================================================================ */

typedef struct {
    _Atomic uint64_t start;   /* start of range being read */
    _Atomic uint32_t part;    /* partition being read (MSEQ_IDLE = idle) */
    uint8_t _pad[MSEQ_CACHE_LINE - 12];
} __attribute__((aligned(MSEQ_CACHE_LINE))) mseq_reader_t;

_Static_assert(sizeof(mseq_reader_t) == MSEQ_CACHE_LINE,
               "mseq_reader_t size must be exactly 1 cache line");

typedef struct {
    mseq_reader_t *readers;
    uint32_t num;
} mseq_rpool_t;

static inline size_t mseq_rpool_size(uint32_t num_consumers) {
    size_t sz = (size_t)num_consumers * sizeof(mseq_reader_t);
    return (sz + MSEQ_CACHE_LINE - 1) & ~(size_t)(MSEQ_CACHE_LINE - 1);
}

static inline int mseq_rpool_init(mseq_rpool_t *rp, void *buf, size_t buf_size,
                                   uint32_t num_consumers) {
    if (!rp || !buf || num_consumers == 0) return -1;
    if (buf_size < mseq_rpool_size(num_consumers)) return -1;
    if ((uintptr_t)buf % MSEQ_CACHE_LINE != 0) return -1;

    memset(buf, 0, buf_size);
    rp->readers = (mseq_reader_t *)buf;
    rp->num = num_consumers;
    for (uint32_t i = 0; i < num_consumers; i++) {
        atomic_store_explicit(&rp->readers[i].part, MSEQ_IDLE, memory_order_relaxed);
        atomic_store_explicit(&rp->readers[i].start,        0, memory_order_relaxed);
    }
    return 0;
}

/* ============================================================================
 * Queue init (zero-allocation)
 * ============================================================================ */

static inline size_t mseq_buffer_size(uint32_t num_parts, uint32_t slots, uint32_t slot_size) {
    size_t parts_sz = (size_t)num_parts * sizeof(mseq_part_t);
    size_t data_sz  = (size_t)num_parts * slots * slot_size;
    size_t total    = parts_sz + data_sz;
    return (total + MSEQ_CACHE_LINE - 1) & ~(size_t)(MSEQ_CACHE_LINE - 1);
}

static inline int mseq_init(mseq_t *q, void *buf, size_t buf_size,
                             uint32_t num_parts, uint32_t slots, uint32_t slot_size) {
    if (!buf || !q) return -1;
    if (num_parts == 0 || (num_parts & (num_parts - 1)) != 0) return -1;
    if (slots == 0     || (slots     & (slots     - 1)) != 0) return -1;
    if (slot_size == 0) return -1;
    if (buf_size < mseq_buffer_size(num_parts, slots, slot_size)) return -1;
    if ((uintptr_t)buf % MSEQ_CACHE_LINE != 0) return -1;

    memset(buf, 0, buf_size);
    q->num_parts      = num_parts;
    q->slots_per_part = slots;
    q->slot_size      = slot_size;
    q->part_mask      = num_parts - 1;

    uint8_t *ptr = (uint8_t *)buf;
    size_t parts_sz = (size_t)num_parts * sizeof(mseq_part_t);
    q->parts = (mseq_part_t *)ptr;
    uint8_t *all_data = ptr + parts_sz;

    for (uint32_t p = 0; p < num_parts; p++) {
        q->parts[p].data      = &all_data[(size_t)p * slots * slot_size];
        q->parts[p].num_slots = slots;
        q->parts[p].slot_size = slot_size;
        q->parts[p].mask      = slots - 1;
        atomic_store_explicit(&q->parts[p].reserved,    0, memory_order_relaxed);
        atomic_store_explicit(&q->parts[p].produce_seq, 0, memory_order_relaxed);
        atomic_store_explicit(&q->parts[p].consume_seq, 0, memory_order_relaxed);
    }
    return 0;
}

/* ============================================================================
 * Producer: reserve / submit
 *
 * Backpressure: checks per-consumer cursors to prevent overwriting
 * data being read. If rpool is NULL, uses consume_seq (unsafe under
 * wrap but simpler for single-consumer or testing).
 * ============================================================================ */

static inline void *mseq_reserve(mseq_t *q, uint32_t p, mseq_rpool_t *rp) {
    if (p >= q->num_parts) return NULL;
    mseq_part_t *part = &q->parts[p];
    if (atomic_load_explicit(&part->reserved, memory_order_relaxed)) return NULL;

    uint64_t head = atomic_load_explicit(&part->produce_seq, memory_order_relaxed);

    /* Find the oldest outstanding read on this partition */
    uint64_t min_safe;  /* oldest outstanding read position on this partition */

    if (rp) {
        /* Gated: scan per-consumer cursors */
        uint64_t cons = atomic_load_explicit(&part->consume_seq, memory_order_acquire);
        min_safe = cons;  /* consume_seq is the baseline (everything before is claimed) */

        for (uint32_t i = 0; i < rp->num; i++) {
            uint32_t rpart = atomic_load_explicit(&rp->readers[i].part, memory_order_acquire);
            if (rpart == p) {
                uint64_t rstart = atomic_load_explicit(&rp->readers[i].start, memory_order_acquire);
                if (rstart < min_safe) min_safe = rstart;
            }
        }
    } else {
        /* Ungated: use consume_seq (fast but unsafe under wrap with slow consumers) */
        min_safe = atomic_load_explicit(&part->consume_seq, memory_order_acquire);
    }

    if ((int64_t)(head - min_safe) >= (int64_t)part->num_slots) return NULL;

    /* API contract: one producer per partition (same thread across reserve/submit).
     * Cross-thread handoff requires external synchronization.
     * The exchange detects violations but does NOT provide ordering for handoff. */
    if (atomic_exchange_explicit(&part->reserved, 1, memory_order_relaxed)) {
#ifdef MSEQ_DEBUG
        fprintf(stderr, "mseq: FATAL — concurrent reserve on partition %u\n", p);
#endif
        return NULL;
    }
    return &part->data[(size_t)((uint32_t)head & part->mask) * part->slot_size];
}

static inline void mseq_submit(mseq_t *q, uint32_t p) {
    if (p >= q->num_parts) return;
    mseq_part_t *part = &q->parts[p];
    if (!atomic_load_explicit(&part->reserved, memory_order_relaxed)) return;

    uint64_t head = atomic_load_explicit(&part->produce_seq, memory_order_relaxed);
    atomic_store_explicit(&part->produce_seq, head + 1, memory_order_release);
    atomic_store_explicit(&part->reserved,           0, memory_order_release);
}

/* ============================================================================
 * Consumer: claim + release
 *
 * claim: CAS on consume_seq + publish cursor on own cache line.
 * release: clear cursor. No done_seq, no spin-wait, no sequential ordering.
 *
 * Each consumer writes ONLY to its own mseq_reader_t cache line.
 * ============================================================================ */

static inline mseq_claim_t mseq_claim(mseq_t *q, mseq_rpool_t *rp, uint32_t cid,
                                       uint32_t range_max) {
    mseq_claim_t c = {NULL, MSEQ_IDLE, 0, 0, 0, 0};

    /* NOTE: tl_start is shared across all mseq_t instances within the same
     * thread. This means round-robin partition scanning state leaks between
     * queues if a thread consumes from multiple queues. Acceptable for the
     * common case (one queue per thread); could be fixed with a per-queue
     * per-thread map if needed. */
    static _Thread_local uint32_t tl_start = 0;
    uint32_t start = tl_start;

    for (uint32_t pi = 0; pi < q->num_parts; pi++) {
        uint32_t p = (start + pi) & q->part_mask;
        mseq_part_t *part = &q->parts[p];

        uint64_t prod = atomic_load_explicit(&part->produce_seq, memory_order_acquire);
        uint64_t cons = atomic_load_explicit(&part->consume_seq, memory_order_relaxed);
        int64_t avail = (int64_t)(prod - cons);
        if (avail <= 0) continue;

        uint32_t n = (uint32_t)avail;
        if (n > range_max) n = range_max;

        /* Pre-publish cursor BEFORE CAS: reserve our spot so the producer
         * sees us even if there's a gap between CAS and data read.
         * If CAS fails, we clear the cursor and retry. */
        if (rp && cid < rp->num) {
            atomic_store_explicit(&rp->readers[cid].start, cons, memory_order_relaxed);
            atomic_store_explicit(&rp->readers[cid].part,     p, memory_order_release);
            /* release on .part above makes both stores visible to producer's
             * acquire loads — no seq_cst fence needed (was costing 10-20 ns). */
        }

        uint64_t desired = cons + n;
        if (!atomic_compare_exchange_strong_explicit(&part->consume_seq, &cons, desired,
                memory_order_acquire, memory_order_relaxed)) {
            /* CAS failed — clear cursor and retry */
            if (rp && cid < rp->num)
                atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE, memory_order_relaxed);
            continue;
        }

        c.data      = &part->data[(size_t)((uint32_t)cons & part->mask) * part->slot_size];
        c.part      = p;
        c.count     = n;
        c.start     = cons;
        c.mask      = part->mask;
        c.start_idx = (uint32_t)cons & part->mask;
        tl_start    = (p + 1) & q->part_mask;
        return c;
    }

    tl_start = (start + 1) & q->part_mask;
    return c;
}

static inline void mseq_release(mseq_t *q, mseq_rpool_t *rp, uint32_t cid,
                                 mseq_claim_t *c) {
    (void)q;
    /* Clear cursor: "I'm done reading" */
    if (rp && cid < rp->num) {
        atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE, memory_order_release);
    }
    c->data = NULL;
    c->part = MSEQ_IDLE;
}

#endif /* MPMC_SEQ_H */
