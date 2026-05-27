# mseq Architecture

This document describes the queue as it stands today: a partitioned,
bounded MPMC queue with zero allocation, two sequence counters per
partition, range-based consumer claims, and per-consumer gated
backpressure. The whole library is header-only
(`include/mpmc_seq.h`, ~320 lines).

The intent is to be the reference for *contracts* between components —
who owns which atomic, which memory ordering is required at which call
site, and what is *not allowed* to cross a given boundary. Per-file
invariants are also inscribed as a block comment at the head of every
header.

---

## 1. Lifecycle

```
caller code
  -> mseq_buffer_size(P, S, slot_size)         # sizing
     allocate aligned(MSEQ_CACHE_LINE) buffer
  -> mseq_init(q, buf, sz, P, S, slot_size)    # zero-allocation init
       validate P, S powers of 2
       memset(buffer)
       lay out: parts[P] then data[P*S*slot_size]
       atomic_init produce_seq / consume_seq / reserved (relaxed)
  -> mseq_rpool_init(rp, rbuf, rsz, C)         # gated mode only
       readers[i].part = MSEQ_IDLE for every i
```

Everything is `static inline` in `include/mpmc_seq.h`. There is no TU to
compile separately. The only error-reporting boundary is `mseq_init` /
`mseq_rpool_init`; after they return 0 the fast path is branch-free
except for "nothing available".

## 2. Memory layout

| Offset inside `mseq_part_t`        | Cache line | Owner            |
|------------------------------------|------------|------------------|
| `data`, `num_slots`, `slot_size`, `mask`, `reserved`, `produce_seq` | line 0 | producer (single) |
| `consume_seq`                      | line 1     | consumers (CAS)  |

Sentinel: `_Static_assert(sizeof(mseq_part_t) == 2 * MSEQ_CACHE_LINE)`
(`mpmc_seq.h`). Any new field must adjust the `_p0`/`_p1` padding or
the build breaks.

```
parts[0]   line 0 producer | line 1 consumer
parts[1]   line 0 producer | line 1 consumer
...
parts[P-1]
data[0 .. P*S*slot_size]                       # flat, partition-major
```

Reader pool layout — one cache line per consumer, single-writer:

| Field    | Width   | Updater     | Reader     |
|----------|---------|-------------|------------|
| `start`  | 8 B     | consumer i  | producer   |
| `part`   | 4 B     | consumer i  | producer   |
| `_pad`   | rest    | -           | -          |

Sentinel: `_Static_assert(sizeof(mseq_reader_t) == MSEQ_CACHE_LINE)`.
`part = MSEQ_IDLE (= UINT32_MAX)` means "this consumer is not currently
inside a claim".

`MSEQ_CACHE_LINE` is fixed at compile time: 128 for
aarch64 / Apple Silicon (cluster-L2 shared), 64 for x86_64. Selected
either by CMake (`CMakeLists.txt`) or by `Makefile.linux` based on
`uname -m`.

## 3. Cost model

The queue's correctness analysis is straightforward (Lamport-style
ring); its performance analysis is governed by the cost of a single
cache-line state transfer under MESI.

| Operation      | Apple Silicon | x86 native | x86 WSL2 |
|----------------|---------------|------------|----------|
| CAS private    |    8 ns       |    -       |    -     |
| CAS 2 cores    |   48 ns       |    -       |    -     |
| CAS 8 cores    |  480 ns       |  ~200 ns   |   80 ns  |
| Load Shared    |    1.1 ns     |    5 ns    |   10 ns  |
| Ratio T_exc/T_sh | ~436×       |  ~40×      |   ~8×    |

This ratio is the invariant that determines which MPMC designs are
viable on which architecture. In particular the announcement protocol
of `include/mseq_announce.h` is profitable iff
`C * T_sh < T_exc(C / P)` — true on Apple Silicon, marginal on x86
(documented negative result; see §11).

Cost per item in `mseq` (gated mode):

```
Producer : 1 store-release on produce_seq + O(C) Shared loads (rpool)
Consumer : 1 CAS / N items on consume_seq + 2 own-line stores (set + clear)
Release  : 1 own-line store (part = MSEQ_IDLE)
```

At N = 16-32 and moderate C this is ≈ 1 shared write per item; the rest
runs in the consumer's own cache lines.

## 4. Producer path

```c
mseq_reserve(q, p, rp):
  if part->reserved          : return NULL          # API guard
  head = part->produce_seq   (relaxed)
  if rp:
    min_safe = part->consume_seq (acquire)
    for r in rp->readers:
      if r.part == p && r.start < min_safe:
        min_safe = r.start
  else:
    min_safe = part->consume_seq (acquire)
  if (head - min_safe) >= num_slots : return NULL   # full
  if atomic_exchange(reserved, 1)   : return NULL   # contract violation
  return &data[(head & mask) * slot_size]

mseq_submit(q, p):
  produce_seq = head + 1  (release)
  reserved    = 0         (release)
```

Critical invariants:

- **One producer per partition.** `reserved` is set with
  `atomic_exchange` (relaxed) — it detects a violation but does **not**
  provide ordering for cross-thread handoff. Same-thread reserve/submit
  is the contract.
- **`produce_seq` advances only at submit.** It is never touched
  between `reserve` and `submit`. The consumer's acquire-load on
  `produce_seq` thus pairs with the producer's release-store *after*
  the payload write, so consumers never observe an uninitialised slot.
- **`reserved` must be 0 on the path out of `mseq_submit`.** Submit
  without a prior reserve is a silent no-op; do not build logic on it.

## 5. Consumer path

```c
mseq_claim(q, rp, cid, range_max):
  for p in round-robin(num_parts):
    prod  = part->produce_seq (acquire)
    cons  = part->consume_seq (relaxed)
    avail = prod - cons
    if avail <= 0: continue
    n = min(avail, range_max)

    # Pre-publish BEFORE CAS:
    if rp:
      rp->readers[cid].start = cons   (relaxed)
      rp->readers[cid].part  = p      (release)

    if !CAS(consume_seq, cons, cons + n)   (acquire on success):
      if rp: rp->readers[cid].part = MSEQ_IDLE (relaxed)
      continue

    return claim_t{ data, part, count, start, mask, start_idx }

mseq_release(q, rp, cid, c):
  if rp: rp->readers[cid].part = MSEQ_IDLE (release)
  c->data = NULL; c->part = MSEQ_IDLE
```

Critical invariants:

- **Pre-publish before CAS, not after.** The `release`-store on
  `readers[cid].part` is issued *before* the CAS on `consume_seq`.
  Reversing the order leaves a wrap window in which the producer sees
  an advanced `consume_seq` but no published reader cursor, and may
  recycle the slot the consumer is about to read.
- **CAS failure clears the cursor.** Otherwise an idle reader stays
  registered on a partition and the producer's gated backpressure
  stalls indefinitely. See the explicit retry/clear in
  `mseq_claim`.
- **`mseq_release` writes only to its own cache line.** No write on
  `consume_seq` and no write on `mseq_part_t` — `consume_seq` was
  already advanced by the CAS in `claim`. Double-advancing it breaks
  backpressure.

Round-robin state across calls:

- `tl_start` is a `_Thread_local static` declared inside `mseq_claim`
  and is therefore shared between all `mseq_t` instances consumed by
  the same thread. Acceptable for the common one-queue-per-thread
  case; topologies with multiple consumed queues per thread will see
  biased (but still correct) round-robin.

## 6. Range claims & ring wrap

A claim of N items can straddle the ring boundary (e.g. slots
`[6,7,0,1]` in an 8-slot ring). The items are then **not contiguous in
memory**.

| `mseq_claim_t` field | Meaning                                              |
|----------------------|------------------------------------------------------|
| `data`               | pointer to the first physical item                   |
| `count`              | number of items in this claim                        |
| `start`              | logical consume_seq at claim time (debug / release)  |
| `mask`               | `num_slots - 1` snapshot at claim time               |
| `start_idx`          | `cons & mask` — base for wrap-safe iteration         |

Critical invariants:

- **`mseq_claim_item(q, c, i)` is the only correct iterator.**
  `claim.data + i * slot_size` is wrong as soon as the claim wraps.
  Documented in the header (see `mpmc_seq.h` doc-comment on the
  function) and the README. Any user-written loop must reproduce
  `((start_idx + i) & mask) * slot_size`.
- **`count <= num_slots`** by construction (clamp to `range_max`,
  itself bounded by `avail = prod - cons <= num_slots`).
- **The payload `[cons, cons + n)` is frozen for the claimer.** The
  producer cannot recycle it as long as `rp->readers[cid].part == p`:
  the gated `min_safe` includes `r.start < cons + n`, and
  `head - min_safe < num_slots` prevents wraparound onto a held range.

## 7. Backpressure modes

Two modes, selected by whether the producer call site passes `rp`:

| Mode    | Backpressure source                                          | Safe under wrap? | Producer cost              |
|---------|--------------------------------------------------------------|------------------|----------------------------|
| Ungated | `consume_seq` (acquire) only                                 | No (slow consumer + wrap = corruption) | 1 acquire load             |
| Gated   | `min(consume_seq, ∀ r in rpool with r.part == p)`            | Yes              | 1 acquire load + O(C) Shared |

Ungated mode is the *measurement ceiling* (highest throughput, no per-
consumer scan) but only correct in SPSC or in test harnesses that
guarantee the ring is drained faster than it is filled.

Not allowed:

- **Mix gated and ungated producers on the same queue.** An ungated
  producer ignores the rpool and will silently overwrite live readers.
- **Reuse a `cid` across two concurrent consumers.** Each consumer
  must own one slot in `mseq_rpool_t`. Sharing a slot lets one
  consumer publish over another's cursor.

## 8. Memory ordering — summary

| Site                                          | Ordering           | Paired with                                           |
|-----------------------------------------------|--------------------|-------------------------------------------------------|
| `mseq_submit`: `produce_seq = h+1`            | release            | `mseq_claim`: acquire-load on `produce_seq`           |
| `mseq_submit`: `reserved = 0`                 | release            | `mseq_reserve`: relaxed-load on `reserved`            |
| `mseq_claim`: pre-publish `readers[cid].part` | release            | `mseq_reserve`: acquire-load on `readers[i].part`     |
| `mseq_claim`: CAS on `consume_seq`            | acquire / relaxed  | future `mseq_claim` retries                           |
| `mseq_release`: `readers[cid].part = IDLE`    | release            | `mseq_reserve`: acquire-load on `readers[i].part`     |

The release on `.part` is sufficient — no `seq_cst` fence is required
— because every producer path that reads `.start` does so *after* an
acquire-load of `.part`. Any `.start` value observed alongside a
matching `.part` is at least as recent as the consumer's pre-publish.

## 9. Submodules & build targets

```
vendor/bench/ck         CK-Ring baseline                       submodule
vendor/bench/folly      Folly MPMC reference                   submodule
vendor/bench/moodycamel ConcurrentQueue (unbounded reference)  submodule
vendor/bench/scq        Nikolaev SCQ (DISC 2019)               submodule
vendor/bench/vyukov     rigtorp/MPMCQueue                      submodule
vendor/test/utest       header-only test framework             submodule
```

Build targets (CMake is primary; `Makefile.linux` is a flat alternative):

| Binary           | Source                          | Vendor deps                |
|------------------|---------------------------------|----------------------------|
| `mseq_test`      | `src/tests/main.c`              | utest                      |
| `bench_compare`  | `src/bench/bench_compare.c`     | ck                         |
| `bench_fair`     | `src/bench/bench_fair.c`        | ck, scq                    |
| `bench_scq`      | `src/bench/bench_scq.c`         | scq                        |
| `bench_latency`  | `src/bench/bench_latency.c`     | -                          |
| `bench_mesi`     | `src/bench/bench_mesi.c`        | -                          |
| `bench_pmu`      | `src/bench/bench_pmu.c`         | `../perf-lite` (sibling repo) |
| `bench_fair_cxx` | `src/bench/bench_fair_cxx.cpp`  | moodycamel + rigtorp (opt-in) |

CMake options:

- `BUILD_CXX_BENCH=ON` — requires
  `vendor/bench/vyukov/include/rigtorp/MPMCQueue.h`.
- `BUILD_TSAN=ON` — also builds `mseq_test_tsan` with
  `-fsanitize=thread -g -O1`.

`bench_pmu` is enabled only if `../perf-lite/src/hwc/hwcounters.c`
exists at configure time; otherwise CMake skips it with a status
message. PMU requires root or `CAP_PERFMON`.

## 10. Tests

`src/tests/main.c` (≈1300 lines, utest framework) defines **29 UTEST
cases**. Grouped by what they exercise:

| Group                             | Cases                                                                                          |
|-----------------------------------|------------------------------------------------------------------------------------------------|
| Lifecycle & validation            | `init`, `init_invalid`, `buffer_size`, `cache_line_config`, `reader_struct_size`               |
| Single-thread API                 | `reserve_submit_claim_release`, `claim_empty`, `fill_and_drain`, `submit_without_reserve`      |
| Range claims                      | `range_claim`, `claim_mask_start_idx`                                                          |
| Ring wrap                         | `wrap_correctness`, `wrap_range_claim`, `wrap_range_with_mask`, `wrap_extreme_u32`             |
| Multi-partition routing           | `multi_partition`, `tl_start_multi_queue`                                                      |
| SPSC threaded                     | `spsc` (50k items, 1P/1C)                                                                      |
| MPMC threaded — correctness       | `no_dup_no_loss` (4P/2C, every item seen exactly once)                                         |
| MPMC threaded — stress            | `stress_8p4c`, `stress_2p8c`, `concurrent_wrap_range_stress`, `multi_consumer_release`, `multi_consumer_active` |
| Gated backpressure                | `backpressure_gated`, `backpressure_concurrent`                                                |
| Announcement protocol             | `announce_basic`, `announce_multi_consumer`, `announce_concurrent`                             |

Notable coverage:

- `wrap_extreme_u32` pre-seeds `produce_seq` / `consume_seq` to just
  below `UINT32_MAX` and runs across the 32-bit boundary, validating
  that slot indexing `(uint32_t)seq & mask` wraps correctly.
- `concurrent_wrap_range_stress` tags every payload with its producer
  partition in the upper 16 bits and asserts that no consumer ever
  reads a tag that disagrees with `c.part` — direct corruption check.
- `tl_start_multi_queue` exercises the documented `tl_start`
  thread-local sharing (cf. §5) and asserts no cross-queue leakage of
  values.
- `reader_struct_size` and `cache_line_config` verify at runtime the
  same invariants enforced at compile time by `_Static_assert`.

The smoke target is `make test` (CMake/CTest equivalent: `ctest`).
A TSan-clean run is required before any commit that touches
`include/mpmc_seq.h` or `include/mseq_announce.h`:

```
cmake -DBUILD_TSAN=ON .. && make mseq_test_tsan && ./build/mseq_test_tsan
```

## 11. Announcement protocol (variant)

`include/mseq_announce.h` provides `mseq_claim_announced`, a drop-in
replacement for `mseq_claim` that trades a contended CAS for one
publish + O(C) Shared loads + an uncontended CAS.

| Architecture     | T_exc / T_sh | Expected speedup        | Measured       |
|------------------|--------------|-------------------------|----------------|
| Apple Silicon    | ~436×        | up to 55× per claim     | wins broadly   |
| x86_64 native    | ~40×         | ~5× for C ≥ 5           | wins moderate  |
| x86_64 WSL2      | ~8-10×       | break-even              | -10% to -20%   |

It is shipped as a separate header so the default path stays
architecture-portable. Selection is a call-site choice; the two
variants cannot be mixed against the same rpool.

## 12. Known footguns

| Risk                                                            | Mitigation                                              | Location                  |
|-----------------------------------------------------------------|----------------------------------------------------------|---------------------------|
| Naive iteration `claim.data + i*ss` past wrap                   | Use `mseq_claim_item`                                    | doc-comment in header     |
| Two producers on the same partition                             | `reserved` detects but does NOT synchronise              | `mseq_reserve`            |
| Gated producer with partially-initialised rpool                 | `mseq_rpool_init` sets every `part` to `MSEQ_IDLE`       | `mseq_rpool_init`         |
| `cid` reused across two consumers                               | Not detected — silent corruption                         | -                         |
| Ungated + slow consumer + wrap                                  | Producer overwrites in-flight reads                      | `mseq_reserve` (rp==NULL) |
| `tl_start` leaks across `mseq_t` instances on the same thread   | Round-robin slightly biased but still correct            | `mseq_claim`              |

## 13. Per-file headers

Every header and every `.c` / `.cpp` source file carries a 5-12 line
block comment stating its single responsibility, its invariants, and
the things it is *not allowed* to do. These are the authoritative
micro-contracts; when modifying a file, read its header first.

| File                              | Role                                                       |
|-----------------------------------|------------------------------------------------------------|
| `include/mpmc_seq.h`              | queue core (partitions, rpool, claim)                      |
| `include/mseq_announce.h`         | pre-CAS announcement variant                               |
| `src/tests/main.c`                | utest suite — 29 cases (see §10)                           |
| `src/bench/bench_compare.c`       | mseq vs CK-Ring, aggregate throughput                      |
| `src/bench/bench_fair.c`          | consumer-only throughput at fixed capacity (paper metric)  |
| `src/bench/bench_fair_cxx.cpp`    | moodycamel + Vyukov references (same methodology, C++)     |
| `src/bench/bench_latency.c`       | p50 / p99 / p999 per-claim latency                         |
| `src/bench/bench_mesi.c`          | physical cost of MESI transitions (cost-model input)       |
| `src/bench/bench_pmu.c`           | mseq throughput with hardware counters (perf-lite)         |
| `src/bench/bench_scq.c`           | mseq vs SCQD (Nikolaev DISC 2019)                          |
| `src/bench/bench_announce.c`      | mseq_claim vs mseq_claim_announced at C >> P               |
| `src/examples/evd_mseq.c`         | event-bus PoC: 4P / 2C with 256-byte events                |
