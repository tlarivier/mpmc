# MPMC — Architecture

This document explains the design principles, data structures, and algorithms behind `mpmc.h`.

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Data Structures](#data-structures)
3. [State Machine](#state-machine)
4. [Memory Ordering](#memory-ordering)
5. [Algorithms](#algorithms)
6. [Performance Analysis](#performance-analysis)
7. [Formal Properties](#formal-properties)

---

## Design Philosophy

### The Problem with Traditional MPMC

Traditional MPMC queues (ConcurrencyKit's ck_ring, LMAX Disruptor) use a **single global atomic counter** for coordination:

```
┌─────────────────────────────────────────────────────────┐
│                    Global Counter                       │
│                         ↓                               │
│    Producer 1 ──┐                                       │
│    Producer 2 ──┼── CAS(head) ── [ Ring Buffer ]        │
│    Producer 3 ──┘                                       │
│                                                         │
│    Consumer 1 ──┐                                       │
│    Consumer 2 ──┼── CAS(tail) ──────────────────────────│
│    Consumer 3 ──┘                                       │
└─────────────────────────────────────────────────────────┘
```

**Problem**: Every producer competes for the same atomic. Under high contention:
- CAS failures increase exponentially
- Cache line bounces between cores
- Throughput collapses as P increases

### The Partitioned Solution

Our design eliminates producer contention by **partitioning**:

```
┌─────────────────────────────────────────────────────────┐
│    Producer 0 ──── [Partition 0: Ring Buffer] ──┐       │
│    Producer 1 ──── [Partition 1: Ring Buffer] ──┼─ Scan │
│    Producer 2 ──── [Partition 2: Ring Buffer] ──┤       │
│    Producer 3 ──── [Partition 3: Ring Buffer] ──┘       │
│                                                 ↓       │
│                                            Consumers    │
└─────────────────────────────────────────────────────────┘
```

**Key insight**: If each producer owns exactly one partition, producers **never contend**. The producer path becomes essentially SPSC (Single-Producer Single-Consumer) with lock-free O(S) bounded complexity.

Consumers scan all partitions, using CAS only to claim individual slots. The consumer complexity is O(P×S), lock-free bounded.

**Note**: This is **lock-free**, not **wait-free**. Operations may return NULL if the queue is full (reserve) or empty (claim). Wait-free would require guaranteed success, which is impossible for a bounded queue.

---

## Data Structures

### Slot

```c
typedef struct {
    _Atomic uint64_t state;  // [62-bit generation] [2-bit status]
} mpmc_slot_t;
```

**Why 8 bytes?**
- Compact: 8 slots per cache line (good prefetching for consumer scans)
- Generation counter prevents ABA problems
- Status encodes state machine position

**Why no padding?**
- Consumer scans benefit from prefetching adjacent slots
- False sharing only matters under extreme contention
- Padding would 8× memory usage

### Partition

```c
typedef struct {
    /* Producer-local (single cache line) */
    mpmc_slot_t *slots;      // Slot array
    uint8_t *data;           // Data array
    uint32_t num_slots;      // Number of slots
    uint32_t slot_size;      // Bytes per slot
    uint32_t mask;           // num_slots - 1 (for fast modulo)
    uint32_t head;           // Next slot to try
    uint32_t current;        // Currently reserved slot
#ifndef NDEBUG
    _Atomic uint64_t owner_tid;  // Debug: detect single-producer violation
#endif
    uint8_t _pad1[...];      // Pad to 64 bytes
} mpmc_part_t;
```

**Design notes**:
- All producer fields fit in one cache line
- No per-partition consumer state (consumers use thread-local rotation)
- Debug mode tracks owner thread ID to detect contract violations

### Queue

```c
typedef struct {
    mpmc_part_t *parts;          // Partition array
    uint32_t num_parts;          // Number of partitions (= producers)
    uint32_t slots_per_part;     // Slots per partition
    uint32_t slot_size;          // Bytes per slot
    uint32_t part_mask;          // num_parts - 1 (for fast modulo)
    uint8_t _pad1[...];          // Pad to 64 bytes
    
    void *mem;                   // Allocated memory block
} mpmc_t;
```

**Design notes**:
- No global consumer state (zero contention between consumers on queue-level atomics)
- Consumers use `_Thread_local` for partition rotation

---

## State Machine

Each slot transitions through 4 states:

```
    ┌─────────────────────────────────────────┐
    │                                         │
    ▼                                         │
 EMPTY ──reserve──▶ FILLING ──submit──▶ READY │
   ▲                    │                 │   │
   │                    │                 │   │
   │               discard             claim  │
   │                    │                 │   │
   │                    ▼                 ▼   │
   └───────────────── EMPTY ◀──release── READING
```

| State | Value | Meaning |
|-------|-------|---------|
| EMPTY | 0 | Slot available for producer |
| FILLING | 1 | Producer is writing data |
| READY | 2 | Data available for consumer |
| READING | 3 | Consumer is reading data |

### State Encoding

```c
uint64_t state = (generation << 2) | status;
```

- **Bits 0-1**: Status (0-3)
- **Bits 2-63**: Generation counter (62 bits)

**ABA Protection**: Generation increments on every transition. At 10⁹ ops/sec/slot, exhaustion takes ~146 years.

---

## Memory Ordering

### Producer Path

```c
// reserve: EMPTY → FILLING
cur = load(relaxed);
CAS(acquire, relaxed);  // acquire syncs with release in mpmc_release (slot reuse)

// submit: FILLING → READY
store(release);  // ← Data must be visible before READY
```

### Consumer Path

```c
// claim: READY → READING
CAS(acquire, relaxed);  // ← Synchronizes with submit's release

// release: READING → EMPTY
store(release);  // ← Ensures consumed data visible before reuse
```

### Synchronization Chain

```
[Data Transfer]
Producer writes data → submit(release) --sync--> claim(acquire) → Consumer reads data

[Slot Reuse]
Consumer done → release(release) --sync--> reserve(acquire) → Producer reuses slot
```

Two release-acquire pairs ensure:
1. **Data visibility**: Consumer sees producer's writes after claim
2. **Safe reuse**: Producer doesn't overwrite data consumer is still reading

---

## Algorithms

### mpmc_reserve — O(S) Lock-Free Bounded

```c
void *mpmc_reserve(mpmc_t *m, uint32_t partition) {
    part = &m->parts[partition];
    if (part->current != UINT32_MAX) return NULL;  // Already reserving
    
    for (i = 0; i < part->num_slots; i++) {
        idx = (part->head + i) & part->mask;
        slot = &part->slots[idx];
        
        cur = load(slot->state, relaxed);
        if ((cur & 3) != EMPTY) continue;
        
        next = (cur & ~3) | FILLING;
        if (CAS(slot->state, cur, next, acquire, relaxed)) {  // acquire!
            part->head = (idx + 1) & part->mask;
            part->current = idx;
            return &part->data[idx * part->slot_size];
        }
    }
    return NULL;  // Full
}
```

**Complexity**: O(S) — bounded loop, no retries on CAS failure.
**Returns NULL** if queue is full (all slots in FILLING/READY/READING state).

### mpmc_claim — O(P×S) Lock-Free Bounded

```c
mpmc_item_t mpmc_claim(mpmc_t *m) {
    static _Thread_local uint32_t tl_start_p = 0;  // Thread-local rotation
    start_p = tl_start_p;
    
    for (pi = 0; pi < m->num_parts; pi++) {
        p = (start_p + pi) & m->part_mask;
        part = &m->parts[p];
        
        for (s = 0; s < part->num_slots; s++) {
            slot = &part->slots[s];
            
            cur = load(slot->state, relaxed);
            if ((cur & 3) != READY) continue;
            
            next = (cur & ~3) | READING;
            if (CAS(slot->state, cur, next, acquire, relaxed)) {
                tl_start_p = (p + 1) & m->part_mask;  // Rotate for next call
                return {data, p, s};
            }
        }
    }
    tl_start_p = (start_p + 1) & m->part_mask;  // Rotate even on failure
    return {NULL, ...};  // Empty
}
```

**Complexity**: O(P×S) — nested bounded loops, no retries on CAS failure.
**Returns NULL** if queue is empty (no slots in READY state).

### mpmc_claim_batch — Amortized Overhead

Batch claiming reduces per-item overhead:

1. Scan all partitions with thread-local rotation
2. Collect up to `max` READY slots, transitioning each to READING via CAS
3. Single pass through all partitions

**Optimization**: Uses same `_Thread_local` rotation as `mpmc_claim`, so repeated batch calls distribute across partitions.

---

## Formal Properties

### Theorem 1: Lock-Free Bounded Producer

The `mpmc_reserve` operation completes in O(S) steps regardless of other threads' actions.

**Proof sketch**:
- Loop iterates at most S times
- CAS failure moves to next slot (no retry)
- No blocking wait

**Note**: This is lock-free, not wait-free. Returns NULL if full.

### Theorem 2: Lock-Free Bounded Consumer

The `mpmc_claim` operation completes in O(P×S) steps regardless of other threads' actions.

**Proof sketch**:
- Outer loop: P iterations
- Inner loop: S iterations each
- CAS failure moves to next slot (no retry)

**Note**: This is lock-free, not wait-free. Returns NULL if empty.

### Theorem 3: Zero Producer Contention

If each partition has exactly one producer, no two producers ever access the same atomic variable.

**Proof sketch**:
- Partition arrays are disjoint
- Each producer accesses only its partition's slots
- No shared mutable state between producers

### Theorem 4: Linearizability

Every operation appears to take effect atomically at some point between its invocation and response.

**Proof sketch**:
- reserve: linearization point at successful CAS (EMPTY→FILLING)
- submit: linearization point at store (FILLING→READY)
- claim: linearization point at successful CAS (READY→READING)
- release: linearization point at store (READING→EMPTY)
