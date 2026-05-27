/*
 * mpmc_seq.h — bounded MPMC queue made of single-producer partitions.
 *
 * Each partition is a ring with two counters: produce_seq, advanced by its
 * producer, and consume_seq, advanced by consumers with a CAS (one CAS can
 * take several items). In gated mode, each consumer also publishes a cursor
 * (partition, start) in a reader pool, and the producer reads those cursors
 * before reusing a slot. Nothing is allocated: the caller provides every
 * buffer. See docs/ARCHITECTURE.md for the reasoning behind each ordering.
 *
 * Invariants:
 *  - sizeof(mseq_part_t) == 2 cache lines and sizeof(mseq_reader_t) == 1
 *    (compile-time asserts).
 *  - num_parts and slots are powers of 2. Buffers are aligned to
 *    MSEQ_CACHE_LINE.
 *  - The consumer stores its cursor (start, then part, both release)
 *    before the CAS on consume_seq, and the CAS is acq_rel. The gated
 *    backpressure depends on this. It is checked with TSan on the test
 *    suite, not formally proven.
 *  - Every mseq_claim path that does not return a claim leaves the cursor
 *    at MSEQ_IDLE.
 *  - consume_seq is only modified by the CAS in mseq_claim.
 *  - cached_min is written only by the producer and never exceeds the
 *    oldest position still being read.
 *  - mseq_buffer_size returns 0 when the sizes overflow size_t.
 *
 * Not allowed:
 *  - Two producers on one partition, or reserve and submit on different
 *    threads without external synchronization. `reserved` catches some
 *    misuse. It is not a lock.
 *  - Two consumers sharing a cid, or a second claim on a cid before the
 *    first one is released.
 *  - rp == NULL with several consumers, or with a consumer that can fall a
 *    full ring behind: slots can be overwritten while being read.
 *  - Gated and ungated producers on the same queue.
 *  - Iterating a claim with claim.data + i * slot_size. Claims can wrap
 *    around the ring, so use mseq_claim_item().
 *  - A slot_size that is not a multiple of the stored type's alignment.
 */

#ifndef MPMC_SEQ_H
#define MPMC_SEQ_H

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#ifdef MSEQ_DEBUG
#include <stdio.h>
#endif

#ifndef MSEQ_CACHE_LINE
#define MSEQ_CACHE_LINE 128
#endif
#define MSEQ_IDLE UINT32_MAX

#if defined(__GCC_ATOMIC_LLONG_LOCK_FREE) && __GCC_ATOMIC_LLONG_LOCK_FREE < 2
#warning "mseq: 64-bit atomics are not lock-free on this target"
#endif

/* ============================================================================
 * Queue structures
 * ============================================================================
 */

typedef struct {
  uint8_t *data;
  uint32_t num_slots;
  uint32_t slot_size;
  uint32_t mask;
  _Atomic uint32_t reserved;
  _Atomic uint64_t produce_seq;
  uint64_t cached_min;
  uint8_t _p0[MSEQ_CACHE_LINE - 40];

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
  void *data;
  uint32_t part;
  uint32_t count;
  uint64_t start;
  uint32_t mask;
  uint64_t start_idx;
} mseq_claim_t;

static inline void *mseq_claim_item(const mseq_t *q, const mseq_claim_t *c,
                                    uint32_t index) {
  if (index >= c->count)
    return NULL;
  if (c->part >= q->num_parts)
    return NULL;
  uint32_t slot = (c->start_idx + index) & c->mask;
  return &q->parts[c->part].data[(size_t)slot * q->slot_size];
}

/* ============================================================================
 * Reader pool
 * ============================================================================
 */

typedef struct {
  _Atomic uint64_t start;
  _Atomic uint32_t part;
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
  if (!rp || !buf || num_consumers == 0)
    return -1;
  if (buf_size < mseq_rpool_size(num_consumers))
    return -1;
  if ((uintptr_t)buf % MSEQ_CACHE_LINE != 0)
    return -1;

  memset(buf, 0, buf_size);
  rp->readers = (mseq_reader_t *)buf;
  rp->num = num_consumers;
  for (uint32_t i = 0; i < num_consumers; i++) {
    atomic_store_explicit(&rp->readers[i].part, MSEQ_IDLE,
                          memory_order_relaxed);
    atomic_store_explicit(&rp->readers[i].start, 0, memory_order_relaxed);
  }
  return 0;
}

/* ============================================================================
 * Queue init
 * ============================================================================
 */

static inline size_t mseq_buffer_size(uint32_t num_parts, uint32_t slots,
                                      uint32_t slot_size) {
  size_t parts_sz, per_part, data_sz, total;
  if (__builtin_mul_overflow((size_t)num_parts, sizeof(mseq_part_t),
                             &parts_sz) ||
      __builtin_mul_overflow((size_t)slots, (size_t)slot_size, &per_part) ||
      __builtin_mul_overflow(per_part, (size_t)num_parts, &data_sz) ||
      __builtin_add_overflow(parts_sz, data_sz, &total) ||
      total > SIZE_MAX - MSEQ_CACHE_LINE)
    return 0;
  return (total + MSEQ_CACHE_LINE - 1) & ~(size_t)(MSEQ_CACHE_LINE - 1);
}

static inline int mseq_init(mseq_t *q, void *buf, size_t buf_size,
                            uint32_t num_parts, uint32_t slots,
                            uint32_t slot_size) {
  if (!buf || !q)
    return -1;
  if (num_parts == 0 || (num_parts & (num_parts - 1)) != 0)
    return -1;
  if (slots == 0 || (slots & (slots - 1)) != 0)
    return -1;
  if (slot_size == 0)
    return -1;
  size_t need = mseq_buffer_size(num_parts, slots, slot_size);
  if (need == 0 || buf_size < need)
    return -1;
  if ((uintptr_t)buf % MSEQ_CACHE_LINE != 0)
    return -1;

  memset(buf, 0, buf_size);
  q->num_parts = num_parts;
  q->slots_per_part = slots;
  q->slot_size = slot_size;
  q->part_mask = num_parts - 1;

  uint8_t *ptr = (uint8_t *)buf;
  size_t parts_sz = (size_t)num_parts * sizeof(mseq_part_t);
  q->parts = (mseq_part_t *)ptr;
  uint8_t *all_data = ptr + parts_sz;

  for (uint32_t p = 0; p < num_parts; p++) {
    q->parts[p].data = &all_data[(size_t)p * slots * slot_size];
    q->parts[p].num_slots = slots;
    q->parts[p].slot_size = slot_size;
    q->parts[p].mask = slots - 1;
    q->parts[p].cached_min = 0;
    atomic_store_explicit(&q->parts[p].reserved, 0, memory_order_relaxed);
    atomic_store_explicit(&q->parts[p].produce_seq, 0, memory_order_relaxed);
    atomic_store_explicit(&q->parts[p].consume_seq, 0, memory_order_relaxed);
  }
  return 0;
}

/* ============================================================================
 * Producer: reserve / submit
 * ============================================================================
 */

static inline uint64_t mseq_oldest_read(mseq_part_t *part, uint32_t p,
                                        mseq_rpool_t *rp) {
  uint64_t min_safe =
      atomic_load_explicit(&part->consume_seq, memory_order_acquire);
  if (!rp)
    return min_safe;

  for (uint32_t i = 0; i < rp->num; i++) {
    uint32_t rpart =
        atomic_load_explicit(&rp->readers[i].part, memory_order_acquire);
    if (rpart != p)
      continue;
    uint64_t rstart =
        atomic_load_explicit(&rp->readers[i].start, memory_order_acquire);
    if (rstart < min_safe)
      min_safe = rstart;
  }
  return min_safe;
}

static inline void *mseq_reserve(mseq_t *q, uint32_t p, mseq_rpool_t *rp) {
  if (p >= q->num_parts)
    return NULL;
  mseq_part_t *part = &q->parts[p];
  if (atomic_load_explicit(&part->reserved, memory_order_relaxed))
    return NULL;

  uint64_t head =
      atomic_load_explicit(&part->produce_seq, memory_order_relaxed);

  if (head - part->cached_min >= part->num_slots) {
    part->cached_min = mseq_oldest_read(part, p, rp);
    if (head - part->cached_min >= part->num_slots)
      return NULL;
  }

  if (atomic_exchange_explicit(&part->reserved, 1, memory_order_relaxed)) {
#ifdef MSEQ_DEBUG
    fprintf(stderr, "mseq: concurrent reserve on partition %u\n", p);
#endif
    return NULL;
  }
  return &part->data[(size_t)((uint32_t)head & part->mask) * part->slot_size];
}

static inline void mseq_submit(mseq_t *q, uint32_t p) {
  if (p >= q->num_parts)
    return;
  mseq_part_t *part = &q->parts[p];
  if (!atomic_load_explicit(&part->reserved, memory_order_relaxed))
    return;

  uint64_t head =
      atomic_load_explicit(&part->produce_seq, memory_order_relaxed);
  atomic_store_explicit(&part->produce_seq, head + 1, memory_order_release);
  atomic_store_explicit(&part->reserved, 0, memory_order_release);
}

/* ============================================================================
 * Consumer: claim / release
 * ============================================================================
 */

static inline mseq_claim_t mseq_claim(mseq_t *q, mseq_rpool_t *rp, uint32_t cid,
                                      uint32_t range_max) {
  mseq_claim_t c = {NULL, MSEQ_IDLE, 0, 0, 0, 0};
  if (range_max == 0)
    return c;

  static _Thread_local uint32_t tl_start = 0;
  uint32_t start = tl_start;

  for (uint32_t pi = 0; pi < q->num_parts; pi++) {
    uint32_t p = (start + pi) & q->part_mask;
    mseq_part_t *part = &q->parts[p];

    uint64_t prod =
        atomic_load_explicit(&part->produce_seq, memory_order_acquire);
    uint64_t cons =
        atomic_load_explicit(&part->consume_seq, memory_order_relaxed);
    int64_t avail = (int64_t)(prod - cons);
    if (avail <= 0)
      continue;

    uint32_t n = (uint32_t)avail;
    if (n > range_max)
      n = range_max;

    if (rp && cid < rp->num) {
      atomic_store_explicit(&rp->readers[cid].start, cons,
                            memory_order_release);
      atomic_store_explicit(&rp->readers[cid].part, p, memory_order_release);
    }

    uint64_t desired = cons + n;
    if (!atomic_compare_exchange_strong_explicit(&part->consume_seq, &cons,
                                                 desired, memory_order_acq_rel,
                                                 memory_order_relaxed)) {
      if (rp && cid < rp->num)
        atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE,
                              memory_order_release);
      continue;
    }

    c.data =
        &part->data[(size_t)((uint32_t)cons & part->mask) * part->slot_size];
    c.part = p;
    c.count = n;
    c.start = cons;
    c.mask = part->mask;
    c.start_idx = (uint32_t)cons & part->mask;
    tl_start = (p + 1) & q->part_mask;
    return c;
  }

  tl_start = (start + 1) & q->part_mask;
  return c;
}

static inline void mseq_release(mseq_t *q, mseq_rpool_t *rp, uint32_t cid,
                                mseq_claim_t *c) {
  (void)q;
  if (rp && cid < rp->num) {
    atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE,
                          memory_order_release);
  }
  c->data = NULL;
  c->part = MSEQ_IDLE;
}

#endif /* MPMC_SEQ_H */
