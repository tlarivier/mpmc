# mseq — Bounded MPMC Queue for Robotics and Embedded Systems

192 lines. Zero allocation. Zero per-slot state. Bounded memory.

Verify: `grep -cvE '^\s*(//|/\*|\*|$)' include/mpmc_seq.h`

## Why

Existing MPMC queues are either:
- Unbounded (moodycamel)          — malloc on the hot path, forbidden in robotics/kernel/firmware
- Slow at scale (CK-Ring, Vyukov) — single-ring designs collapse under contention
- Complex (LCRQ, SCQ)             — hundreds of lines, hard to audit for safety-critical use

mseq is a bounded, zero-allocation MPMC queue that:
- Uses 2 sequence counters per partition instead of per-slot state machines
- Achieves 84 Mops/s at 4P/1C gated (vs CK-Ring: 5.9, SCQD: 7.0) — 14.5× CK-Ring
- Zero-allocation bounded design suitable for safety-critical systems
- Has zero malloc/free — the caller provides a pre-allocated buffer

## Design

```
Producer 0 ──▶ [produce_seq₀ ████████ consume_seq₀] ──┐
Producer 1 ──▶ [produce_seq₁ ██░░░░░░ consume_seq₁] ──┼──▶ Consumers (CAS on consume_seq)
Producer 2 ──▶ [produce_seq₂ █████░░░ consume_seq₂] ──┘
```

No per-slot state. Items between `[consume_seq, produce_seq)` are readable.
Producer: 1 release-store per item. Consumer: 1 CAS per N items (range claim).

## Usage

```c
#include "mpmc_seq.h"

// Zero-allocation: caller provides the buffer
size_t sz = mseq_buffer_size(4, 256, 64);
uint8_t buf[sz] __attribute__((aligned(MSEQ_CACHE_LINE)));

mseq_t q;
mseq_init(&q, buf, sizeof(buf), 4, 256, 64);  // 4 parts, 256 slots, 64B each

// Producer (partition = producer ID)
void *slot = mseq_reserve(&q, 0, &rpool);
if (slot) {
    memcpy(slot, &sensor_data, sizeof(sensor_data));
    mseq_submit(&q, 0);
}

// Consumer (range claim: 1 CAS for N items)
// NOTE: range claims can wrap around the ring — items are NOT contiguous.
// Use mseq_claim_item() or manual wrap with c.start_idx/c.mask.
mseq_claim_t c = mseq_claim(&q, &rpool, cid, 16);  // claim up to 16 items
if (c.data) {
    for (uint32_t i = 0; i < c.count; i++) {
        sensor_data_t *msg = (sensor_data_t *)mseq_claim_item(&q, &c, i);
        process(msg);
    }
    mseq_release(&q, &rpool, cid, &c);
}
```

## API

```c
// Size calculation (for static allocation)
size_t mseq_buffer_size(uint32_t num_parts, uint32_t slots, uint32_t slot_size);

// Init (zero-allocation: caller provides aligned buffer)
int mseq_init(mseq_t *q, void *buf, size_t buf_size,
              uint32_t num_parts, uint32_t slots, uint32_t slot_size);

// Producer (single-writer per partition)
void *mseq_reserve(mseq_t *q, uint32_t partition, mseq_rpool_t *rp);
void  mseq_submit(mseq_t *q, uint32_t partition);

// Consumer (range-based, 1 CAS per N items)
mseq_claim_t mseq_claim(mseq_t *q, mseq_rpool_t *rp, uint32_t cid, uint32_t range_max);
void         mseq_release(mseq_t *q, mseq_rpool_t *rp, uint32_t cid, mseq_claim_t *c);
```

## Benchmarks — Gated Mode (Apple Silicon, 128-byte cache lines)

Consumer throughput only (items dequeued/sec). mseq uses gated mode
(per-consumer cursors) for correctness. All bounded designs use 4096 total slots.

| Config  | mseq R=1 | mseq R=16 | mseq R=32 | SCQD | CK-Ring |
|---------|----------|-----------|-----------|------|---------|
| 1P/1C   | 8.6      | 15.2      | 14.8      | 12.3 | 98.9    |
| 2P/2C   | 4.8      | 25.2      | 23.9      | 4.6  | 41.4    |
| 4P/4C   | 6.4      | 21.0      | 21.0      | 5.9  | 27.5    |
| 8P/8C   | 5.3      | 11.7      | 12.6      | 5.5  | 0.0     |
| 16P/16C | 3.8      | 9.1       | 10.1      | 5.6  | 0.0     |
| 4P/1C   | 4.1      | 54.5      | 84.1      | 7.0  | 5.9     |
| 4P/8C   | 3.9      | 5.0       | 4.6       | 4.9  | 29.6    |

*All values in Mops/s. CK-Ring crashes at 8P/8C+. See src/bench/bench_fair.c for methodology.
moodycamel (unbounded, uses malloc) is excluded — see bench_fair.c header for rationale.*

## Constraints

| Rule                                                  | Reason                   |
|-------------------------------------------------------|--------------------------|
| 1 producer per partition                              | Zero producer contention |
| Power-of-2 slots and partitions                       | Fast modulo via bitmask  |
| Buffer must be aligned to `MSEQ_CACHE_LINE`           | Cache line isolation     |
| `MSEQ_CACHE_LINE` = 128 for Apple Silicon, 64 for x86 | Avoid false sharing      |

## Further reading

- `docs/ARCHITECTURE.md` — subsystem boundaries, invariants, data flow, cost model

## Project structure

```
mpmc/
├── include/mpmc_seq.h          # The queue (192 lines, header-only)
├── src/
│   ├── tests/main.c            # 29 tests (correctness, stress, SPSC, MPMC)
│   ├── bench/
│   │   ├── bench_compare.c     # mseq vs CK-Ring
│   │   ├── bench_fair.c        # Fair throughput (gated, consumer-only)
│   │   ├── bench_fair_cxx.cpp  # Fair throughput (moodycamel, Vyukov)
│   │   ├── bench_latency.c     # Latency percentiles (p50/p99/p999)
│   │   ├── bench_mesi.c        # MESI cost measurement
│   │   ├── bench_pmu.c         # PMU-instrumented (Linux only)
│   │   └── bench_scq.c         # mseq vs SCQ (Nikolaev DISC 2019)
│   └── examples/
│       └── evd_mseq.c          # Event bus proof-of-concept
├── docs/                        # Research documentation
├── legacy/                      # Previous designs (historical)
└── vendor/                      # CK-Ring, Vyukov, moodycamel (submodules)
```

## Build

```bash
mkdir build && cd build && cmake .. && make -j

./mseq_test      # 29 tests
./bench_compare   # mseq vs CK-Ring (Mops/s)
./bench_mesi      # MESI cost matrix (ns/op)
```
