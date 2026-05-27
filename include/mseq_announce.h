/*
 * mseq_announce.h — alternative to mseq_claim() for many consumers per
 *                   partition.
 *
 * Before the CAS, a consumer announces its target (partition, consume_seq)
 * through its reader cursor, then reads the other cursors. If a lower cid
 * targets the same position, it moves on to the next partition. The
 * announcement is a hint only: when two announcements cross, both
 * consumers try the CAS and one of them fails. Whether this is faster than
 * mseq_claim() depends on the hardware. On our measurements it was faster
 * on Apple Silicon and 10 to 20% slower under WSL2.
 *
 * Invariants:
 *  - Same cursor ordering as mseq_claim(): start and part stored with
 *    release before an acq_rel CAS.
 *  - Every path that does not return a claim leaves the cursor at
 *    MSEQ_IDLE.
 *
 * Not allowed:
 *  - Mixing mseq_claim() and mseq_claim_announced() on the same rpool.
 *  - rp == NULL or cid >= rp->num (returns an empty claim).
 *  - Everything listed under "Not allowed" in mpmc_seq.h.
 */

#ifndef MSEQ_ANNOUNCE_H
#define MSEQ_ANNOUNCE_H

#include "mpmc_seq.h"

static inline mseq_claim_t mseq_claim_announced(mseq_t *q, mseq_rpool_t *rp,
                                                  uint32_t cid,
                                                  uint32_t range_max) {
    mseq_claim_t c = {NULL, MSEQ_IDLE, 0, 0, 0, 0};
    if (!rp || cid >= rp->num || range_max == 0) return c;

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

        atomic_store_explicit(&rp->readers[cid].start, cons, memory_order_release);
        atomic_store_explicit(&rp->readers[cid].part,     p, memory_order_release);

        int should_backoff = 0;
        for (uint32_t j = 0; j < cid; j++) {
            uint32_t their_part = atomic_load_explicit(&rp->readers[j].part,
                                                        memory_order_acquire);
            if (their_part != p) continue;

            uint64_t their_seq = atomic_load_explicit(&rp->readers[j].start,
                                                       memory_order_relaxed);
            if (their_seq == cons) {
                should_backoff = 1;
                break;
            }
        }

        if (should_backoff) {
            atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE,
                                  memory_order_release);
            continue;
        }

        uint64_t desired = cons + n;
        if (!atomic_compare_exchange_strong_explicit(&part->consume_seq,
                &cons, desired,
                memory_order_acq_rel, memory_order_relaxed)) {
            atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE,
                                  memory_order_release);
            continue;
        }

        c.data      = &part->data[(size_t)((uint32_t)cons & part->mask) * part->slot_size];
        c.part      = p;
        c.count     = n;
        c.start     = cons;
        c.mask      = part->mask;
        c.start_idx = (uint32_t)cons & part->mask;
        tl_start = (p + 1) & q->part_mask;
        return c;
    }

    tl_start = (start + 1) & q->part_mask;
    return c;
}

#endif /* MSEQ_ANNOUNCE_H */
