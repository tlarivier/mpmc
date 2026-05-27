/*
 * bench_common.h — helpers shared by the mseq benchmarks.
 *
 * Invariants:
 *  - mseq_bench_init builds a gated queue (queue + reader pool) and splits
 *    total_slots across the partitions, with at least 8 slots each.
 *  - mseq_bench_read reads every item of a claim. Its result goes to
 *    mseq_bench_sink so the compiler keeps the reads.
 *
 * Not allowed:
 *  - Counting a claim in a benchmark without reading its items: the other
 *    queues in the comparison read theirs.
 *  - Benchmarking mseq ungated (rp == NULL).
 */

#ifndef MSEQ_BENCH_COMMON_H
#define MSEQ_BENCH_COMMON_H

#include <stdlib.h>
#include "mpmc_seq.h"

static inline uint64_t mseq_bench_read(const mseq_t *q, const mseq_claim_t *c) {
    uint64_t sum = 0;
    for (uint32_t i = 0; i < c->count; i++)
        sum += *(const uint64_t *)mseq_claim_item(q, c, i);
    return sum;
}

static inline void mseq_bench_sink(uint64_t v) {
    __asm__ volatile("" : : "r"(v));
}

static inline uint32_t mseq_bench_pow2(uint32_t v) {
    if (v <= 1) return 1;
    v--; v |= v >> 1; v |= v >> 2; v |= v >> 4; v |= v >> 8; v |= v >> 16;
    return v + 1;
}

typedef struct {
    mseq_t q;
    mseq_rpool_t rp;
    void *qbuf, *rbuf;
} mseq_bench_t;

static inline int mseq_bench_init(mseq_bench_t *b, uint32_t parts,
                                  uint32_t total_slots, uint32_t consumers) {
    uint32_t per_part = total_slots / parts;
    if (per_part < 8) per_part = 8;
    if (consumers == 0) consumers = 1;

    size_t qsz = mseq_buffer_size(parts, per_part, sizeof(uint64_t));
    size_t rsz = mseq_rpool_size(consumers);
    b->qbuf = aligned_alloc(MSEQ_CACHE_LINE, qsz);
    b->rbuf = aligned_alloc(MSEQ_CACHE_LINE, rsz);
    if (!b->qbuf || !b->rbuf) return -1;
    if (mseq_init(&b->q, b->qbuf, qsz, parts, per_part, sizeof(uint64_t)))
        return -1;
    return mseq_rpool_init(&b->rp, b->rbuf, rsz, consumers);
}

static inline void mseq_bench_free(mseq_bench_t *b) {
    free(b->qbuf);
    free(b->rbuf);
}

#endif /* MSEQ_BENCH_COMMON_H */
