/*
 * mseq_announce.h — pre-CAS announcement protocol: write intent on own
 *                   cache line, scan peers, back off if a lower-id peer
 *                   targets the same (partition, consume_seq), then CAS
 *                   uncontended.
 *
 * Trades O(C/P) contended CAS for O(1) write + O(C) shared loads.
 * Reuses rp->readers[cid] for intent storage (no extra cache lines).
 *
 * Invariants:
 *  - .part is published release BEFORE peers are scanned; the scan uses
 *    acquire loads so both .part and .start of every peer are coherent.
 *  - Deterministic priority: on tie (same partition, same consume_seq)
 *    the lower cid wins. No livelock — at most one consumer per (p, seq).
 *  - On backoff OR CAS failure, .part is restored to MSEQ_IDLE before
 *    moving to the next partition (otherwise the producer's gated
 *    backpressure sees a phantom reader and stalls).
 *
 * Not allowed:
 *  - Use on architectures with T_exc/T_sh < ~20 (e.g. x86_64 WSL2) —
 *    overhead of C shared loads exceeds the saved CAS cost. Measured
 *    regression: -10% to -20%.
 *  - Mix mseq_claim() and mseq_claim_announced() on the same rpool —
 *    both reuse .part, but the announce variant relies on peer-scan
 *    semantics that the plain claim does not maintain.
 *  - Call with rp == NULL or cid >= rp->num (function returns empty claim
 *    silently; not an assertion).
 */

#ifndef MSEQ_ANNOUNCE_H
#define MSEQ_ANNOUNCE_H

#include "mpmc_seq.h"

/*
 * Announcement-based claim: reduces CAS contention via pre-CAS coordination.
 *
 * Same interface as mseq_claim(), drop-in replacement.
 * Requires rpool != NULL and cid < rpool->num.
 *
 * The rpool->readers[cid] struct is extended in-place:
 *   .part  = target partition (or MSEQ_IDLE)
 *   .start = target consume_seq value
 *
 * These are already published by the standard claim protocol for
 * backpressure purposes. We reuse them for inter-consumer coordination.
 */
static inline mseq_claim_t mseq_claim_announced(mseq_t *q, mseq_rpool_t *rp,
                                                  uint32_t cid,
                                                  uint32_t range_max) {
    mseq_claim_t c = {NULL, MSEQ_IDLE, 0, 0, 0, 0};
    if (!rp || cid >= rp->num) return c;

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

        /* ── Phase 1: Announce intent ──────────────────────────────
         * Publish: "I (cid) intend to claim from partition p at seq cons"
         * Uses the existing rpool reader slot (already cache-line aligned,
         * written only by this consumer). */
        atomic_store_explicit(&rp->readers[cid].start, cons, memory_order_relaxed);
        atomic_store_explicit(&rp->readers[cid].part,     p, memory_order_release);
        /* release on .part ensures both .start and .part are visible
         * before we read others' intents. */

        /* ── Phase 2: Scan other consumers' intents ────────────────
         * If a consumer with lower cid targets the same partition and
         * the same consume_seq, we yield to it (deterministic priority).
         * Cost: (C-1) shared loads ≈ (C-1) × 1.1 ns */
        int should_backoff = 0;
        for (uint32_t j = 0; j < rp->num; j++) {
            if (j == cid) continue;
            uint32_t their_part = atomic_load_explicit(&rp->readers[j].part,
                                                        memory_order_acquire);
            if (their_part != p) continue;

            uint64_t their_seq = atomic_load_explicit(&rp->readers[j].start,
                                                       memory_order_relaxed);
            if (their_seq == cons && j < cid) {
                /* Lower-ID consumer targets same slot → yield */
                should_backoff = 1;
                break;
            }
        }

        if (should_backoff) {
            /* Clear intent and try next partition */
            atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE,
                                  memory_order_relaxed);
            continue;
        }

        /* ── Phase 3: CAS (should be uncontended) ──────────────── */
        uint64_t desired = cons + n;
        if (!atomic_compare_exchange_strong_explicit(&part->consume_seq,
                &cons, desired,
                memory_order_acquire, memory_order_relaxed)) {
            /* CAS still failed — another consumer slipped through.
             * This can happen if the announcement was too late
             * (race between announce and CAS). */
            atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE,
                                  memory_order_relaxed);
            continue;
        }

        /* Success — fill claim struct */
        c.data      = &part->data[(size_t)((uint32_t)cons & part->mask) * part->slot_size];
        c.part      = p;
        c.count     = n;
        c.start     = cons;
        c.mask      = part->mask;
        c.start_idx = (uint32_t)cons & part->mask;
        tl_start = (p + 1) & q->part_mask;
        return c;
    }

    /* No partition had available items */
    atomic_store_explicit(&rp->readers[cid].part, MSEQ_IDLE,
                          memory_order_relaxed);
    tl_start = (start + 1) & q->part_mask;
    return c;
}

#endif /* MSEQ_ANNOUNCE_H */
