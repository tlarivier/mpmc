# mseq architecture

Invariants and forbidden uses are listed in the header of
`include/mpmc_seq.h`. This file covers the rest.

## Layout

- Partition = 2 cache lines. Line 0 holds the producer's fields
  (`produce_seq`, `reserved`, `cached_min`, ring geometry). Line 1 holds
  only `consume_seq`.
- Reader pool = 1 cache line per consumer: `start`, `part` (`MSEQ_IDLE`
  when not reading).
- Buffer: partition array, then slot data, partition after partition.

## Algorithm

```
reserve(p):
  head = produce_seq
  if head - cached_min >= slots:
    cached_min = min(consume_seq, start of readers with part == p)
    if head - cached_min >= slots: full
  return slot[head & mask]
submit(p):   produce_seq = head + 1                       (release)

claim(cid):
  for each partition p (round-robin):
    n = min(produce_seq - consume_seq, range_max)
    reader.start = cons; reader.part = p                  (release, release)
    CAS(consume_seq, cons, cons + n)                      (acq_rel)
    on failure: reader.part = IDLE                        (release)
release(cid): reader.part = IDLE                          (release)
```

`cached_min` assumes the oldest read position only moves forward, so an
old value underestimates it. This is tested, not proven.

## Memory ordering

- `produce_seq` release / acquire: the payload is visible before the item.
- Cursor stores before an acq_rel CAS: a producer that reads the new
  `consume_seq` should also see the cursor (C11 model). With an
  acquire-only CAS, the cursor stores may be reordered after it.
- `start` release: the producer may pair an old `part` with a newer
  `start`. Release orders the consumer's previous reads before that store.
  TSan reported a race here when `start` was relaxed.
- `part = IDLE` release: slot reads happen before the producer sees the
  cursor go idle.

## Cost

| | Apple Silicon | x86 | WSL2 |
|---|---|---|---|
| CAS, 8 cores | 480 ns | ~200 ns | 80 ns |
| Shared load | 1.1 ns | 5 ns | 10 ns |

Measured with `bench_mesi`. Per item: 1 release store for the producer
(plus a cursor scan when the ring looks full), and 1 CAS per n items plus
2 own-line stores for the consumer. `mseq_announce.h` swaps the contended
CAS for C shared loads. In our runs it was faster on Apple Silicon and
slower under WSL2.

## Tests

32 utest cases. The threaded ones run gated. Producers write
`TAG(partition, position)`, and consumers check each item after the claim
and before the release. TSan (`make tsan`) runs in CI.

## Build

`Makefile` + `rules/` (toolchain, functions, compile, link, help). Output in
`build/obj` and `build/bin`. `make` builds `mseq_test`, the benchmarks
(`bench_fair` gives the README numbers) and `evd_mseq`. `bench_pmu` is
built only if `../perf-lite` exists. `make cxx` builds `bench_fair_cxx`.
`CACHE_LINE` defaults to 128 on aarch64 and 64 elsewhere.
