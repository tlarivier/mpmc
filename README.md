# MPMC — Lock-Free Partitioned Queue for Robotics

**Header-only. Zero dependencies. 102 tests. Benchmarked.**

---

## Features

- **Lock-free operations** — At least one thread always makes progress
- **Zero producer contention** — Each producer has its own partition, no shared atomics
- **Header-only implementation** — Just `#include "mpmc.h"` and go
- **Bulk operations** — Batch claim/release

## IMPORTANT 

This projet is only experimental. DO NOT USE IN PRODUCTION !

## Reasons to use

Most lock-free MPMC queues (Boost, TBB, moodycamel) use a **global atomic counter** shared by all producers. Under contention, this becomes a bottleneck.

This queue uses a **partitioned design**: each producer owns a dedicated ring buffer. Consumers scan all partitions, but producers never contend with each other.

## Reasons NOT to use

- **Not linearizable** — Elements from different producers have no global order. If you need strict FIFO across all producers, use a different queue.
- **1 producer per partition** — The design assumes independent producers. If your producers coordinate, you lose the contention benefits.
- **Requires C11** — Uses `<stdatomic.h>`. Won't compile on ancient compilers.

If all you need is a single-producer single-consumer queue, a simple ring buffer will be faster.

## Design

```
Producer 0 ──▶ [Partition 0: ████████] ──┐
Producer 1 ──▶ [Partition 1: ██░░░░░░] ──┼──▶ Consumer (scans all)
Producer 2 ──▶ [Partition 2: █████░░░] ──┘
```

Each partition is an independent ring buffer with per-slot state tracking:

```
Slot states: FREE → RESERVED → READY → CLAIMED → FREE
                 ↑              ↑           ↑
              Producer      Producer    Consumer
```

**Key invariants:**
- Only 1 producer writes to a partition
- Consumers compete via CAS on READY→CLAIMED transition
- Thread-local rotation distributes consumers across partitions

This design eliminates the "thundering herd" problem where all threads fight over a single counter.

## Basic usage

```c
#include "mpmc.h"

mpmc_t queue;
mpmc_init(&queue, 4, 256, 64);  // 4 partitions, 256 slots, 64B each

// Producer thread (partition = thread ID)
void* slot = mpmc_reserve(&queue, 0);
if (slot) {
    memcpy(slot, &my_data, sizeof(my_data));
    mpmc_submit(&queue, 0);
}

// Consumer thread (any)
mpmc_item_t item = mpmc_claim(&queue);
if (item.data) {
    process(item.data);
    mpmc_release(&queue, &item);
}

mpmc_destroy(&queue);
```

**Important:** Ensure the queue is fully constructed before use by other threads. Similarly, ensure all threads have finished before calling `mpmc_destroy()`.

## Full API

```c
// ============ Lifecycle ============
int  mpmc_init(mpmc_t *q, uint32_t partitions, uint32_t slots, uint32_t slot_size);
void mpmc_destroy(mpmc_t *q);

// ============ Producer (lock-free, O(S) worst case) ============
void* mpmc_reserve(mpmc_t *q, uint32_t partition);   // Returns slot or NULL if full
void  mpmc_submit(mpmc_t *q, uint32_t partition);    // Publish reserved slot
void  mpmc_discard(mpmc_t *q, uint32_t partition);   // Cancel reservation

// ============ Consumer (lock-free, O(P×S) worst case) ============
mpmc_item_t mpmc_claim(mpmc_t *q);                   // Returns {.data=NULL} if empty
void        mpmc_release(mpmc_t *q, mpmc_item_t *item);

// ============ Batch operations (high throughput) ============
int  mpmc_claim_batch(mpmc_t *q, mpmc_batch_t *batch, uint32_t max);
void mpmc_release_batch(mpmc_t *q, mpmc_batch_t *batch);

// ============ Partition-specific consumer ============
mpmc_item_t mpmc_claim_partition(mpmc_t *q, uint32_t partition);
```

## Constraints

| Rule | Reason |
|------|--------|
| **1 producer per partition** | Eliminates producer contention |
| **1 reserve at a time** | Call `submit()` or `discard()` before next `reserve()` |
| **Slots must be power of 2** | Enables fast modulo via bitmask |
| **Partitions must be power of 2** | Enables fast modulo via bitmask |

## Other's features

### Bulk operations

For maximum throughput, use batch operations:

```c
mpmc_batch_t batch;
int count = mpmc_claim_batch(&queue, &batch, 64);  // Claim up to 64 items

for (int i = 0; i < count; i++) {
    process(batch.items[i].data);
}

mpmc_release_batch(&queue, &batch);  // Release all at once
```

### Partition-specific consumption

If you know which producer you want to consume from:

```c
// Only consume from partition 0 (useful for priority lanes)
mpmc_item_t item = mpmc_claim_partition(&queue, 0);
```

### Zero-copy pattern

```c
// Reserve slot, write directly, submit — no intermediate copy
sensor_data_t* slot = (sensor_data_t*)mpmc_reserve(&queue, producer_id);
if (slot) {
    slot->timestamp = now();
    slot->value = read_sensor();
    mpmc_submit(&queue, producer_id);
}
```

## Included modules

| Header | Purpose |
|--------|---------|
| `mpmc.h` | Core lock-free partitioned queue |
| `mpmc_numa.h` | NUMA-aware allocation and local-first scanning (Linux) |

### NUMA support

On multi-socket systems, use `mpmc_numa.h` for optimal memory locality:

```c
#include "mpmc_numa.h"

mpmc_numa_t queue;
mpmc_numa_config_t config = {
    .num_nodes           = mpmc_numa_get_node_count(),
    .partitions_per_node = 4,
    .slots_per_partition = 256,
    .slot_size           = 64
};
mpmc_numa_init(&queue, &config);

// Producer: partition allocated on local NUMA node
void* slot = mpmc_numa_reserve(&queue, partition_id);
mpmc_numa_submit(&queue, partition_id);

// Consumer: scans local node first, then remote
mpmc_item_t item = mpmc_numa_claim_local_first(&queue);
```

Requires `libnuma` on Linux. Falls back to standard allocation on other platforms.

## Build & Test

```bash
# Build everything
mkdir build && cd build
cmake .. && make -j

# Run tests
./mpmc_test           # 102 unit tests

# Run examples
./example_basic       # Basic usage demo
./example_pipeline    # Multi-stage pipeline

# Run benchmarks
./benchmark           # Comparative benchmark
```

## Limitations & assumptions

This section is important for safety-critical applications:

| Aspect | Guarantee | Limitation |
|--------|-----------|------------|
| **Progress** | Lock-free (system-wide progress) | Not wait-free: operations may return NULL if queue full/empty |
| **Ordering** | FIFO per partition | No global FIFO across partitions |
| **Memory** | Single allocation at init | Not NUMA-aware |
| **Destruction** | Must ensure no active users | UB if destroyed while in use |
| **Timing** | O(P×S) worst case | Not hard real-time (no WCET proof) |

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — Internal design & memory ordering
- [docs/PROOF.pdf](docs/PROOF.pdf) — Mathematical proofs (LGN, Hoeffding)

## Project structure

```
mpmc/
├── include/
│   ├── mpmc.h              # Core queue
│   ├── mpmc_numa.h         # Numa awarness
├── src/
│   ├── tests/              # 102 tests
│   ├── examples/           # Usage examples
│   └── bench/              # Benchmarks
├── docs/                   # Documentation
└── README.md
```

