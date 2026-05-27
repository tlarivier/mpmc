# mseq

Bounded MPMC queue, header-only C11, no allocation (`include/mpmc_seq.h`).

One ring per producer, two counters per ring. Consumers take ranges of
items with one CAS. Order is kept per producer, not globally. Details in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Usage rules are in the
header comment of `mpmc_seq.h`.

## Usage

```c
mseq_t q; mseq_rpool_t rp;
mseq_init(&q, qbuf, mseq_buffer_size(4, 256, 64), 4, 256, 64);
mseq_rpool_init(&rp, rbuf, mseq_rpool_size(2), 2);

void *slot = mseq_reserve(&q, p, &rp);          /* producer of partition p */
if (slot) { memcpy(slot, &msg, sizeof msg); mseq_submit(&q, p); }

mseq_claim_t c = mseq_claim(&q, &rp, cid, 16);  /* consumer cid */
for (uint32_t i = 0; i < c.count; i++)
    process(mseq_claim_item(&q, &c, i));
if (c.data) mseq_release(&q, &rp, cid, &c);
```

Buffers are provided by the caller and aligned to `MSEQ_CACHE_LINE`.

## Performance

Consumer throughput (Mops/s), `bench_fair`, M1 Pro 8 cores, 4096 slots in
total, one run per configuration.

| Config | mseq R=32 | SCQD | CK-Ring | CK-Part |
|---|---|---|---|---|
| 1P/1C | 11.5 | 11.1 | 69.7 | 58.4 |
| 4P/4C | 17.4 | 5.5 | 28.0 | 10.8 |
| 4P/1C | 102.5 | 6.6 | 5.3 | 9.9 |
| 16P/1C | 25.6 | 0.0 | 0.0 | 4.6 |
| 4P/8C | 6.2 | 4.5 | 30.5 | 9.3 |
| 1P/32C | 0.2 | 1.0 | 31.1 | 16.2 |

In these runs, mseq was ahead with more producers than consumers, and
behind elsewhere, by a wide margin when consumers outnumber partitions.
CK-Part is CK-Ring with one ring per producer. Full grid: `./bench_fair`.

## Build

```bash
git submodule update --init vendor/test/utest
cmake -B build -DBUILD_TSAN=ON && cmake --build build && (cd build && ctest)
```

Benchmarks need the other submodules. `Makefile.linux` is the CMake-free
alternative used by CI.
