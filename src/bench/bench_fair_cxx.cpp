/*
 * bench_fair_cxx.cpp — C++ reference companions to bench_fair.c:
 *                      moodycamel::ConcurrentQueue (unbounded) and
 *                      rigtorp::MPMCQueue (Vyukov bounded). Same
 *                      methodology: consumer-only throughput, 4096
 *                      capacity, 2-second window, 100ms warmup.
 *
 * Invariants:
 *  - moodycamel is unbounded; we report it because it is the standard
 *    "no-malloc-in-hotpath-claimed" reference. Its memory growth is
 *    out-of-band and not visible in this bench.
 *  - Vyukov is bounded at exactly CAPACITY slots — matches the bounded
 *    designs in bench_fair.c.
 *
 * Not allowed:
 *  - Add bounded SCQ / CK-Ring here — they live in bench_fair.c
 *    (separate binary so the C and C++ toolchains stay decoupled).
 *  - Use moodycamel without ProducerToken / ConsumerToken — the doc
 *    explicitly recommends them and removing them shifts the result.
 */
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <atomic>
#include <thread>
#include <vector>
#include <chrono>

#include "concurrentqueue.h"       // moodycamel
#include "rigtorp/MPMCQueue.h"     // Vyukov

#define CAPACITY 4096
#define BENCH_SEC 2

template<typename ProdFn, typename ConsFn>
static double run_fair(ProdFn pfn, ConsFn cfn, int np, int nc) {
    std::atomic<bool> running{true};
    std::atomic<uint64_t> consumed{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < np; i++)
        threads.emplace_back([&, i]() { pfn(i, running); });
    for (int i = 0; i < nc; i++)
        threads.emplace_back([&, i]() {
            uint64_t n = cfn(i, running);
            consumed.fetch_add(n);
        });

    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // warmup
    consumed.store(0);

    auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(BENCH_SEC));
    auto t1 = std::chrono::steady_clock::now();
    running.store(false);

    for (auto &t : threads) t.join();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    return consumed.load() / elapsed / 1e6;
}

/* ========================================================================
 * moodycamel (unbounded, with tokens)
 * ======================================================================== */

static double bench_moodycamel(int np, int nc) {
    moodycamel::ConcurrentQueue<uint64_t> q(CAPACITY);
    std::vector<moodycamel::ProducerToken> ptoks;
    for (int i = 0; i < np; i++) ptoks.emplace_back(q);

    auto prod = [&](int id, std::atomic<bool> &run) {
        uint64_t v = (uint64_t)id << 48;
        while (run.load(std::memory_order_relaxed))
            q.enqueue(ptoks[id], v++);
    };

    auto cons = [&](int id, std::atomic<bool> &run) -> uint64_t {
        moodycamel::ConsumerToken ct(q);
        uint64_t v, n = 0;
        while (run.load(std::memory_order_relaxed))
            if (q.try_dequeue(ct, v)) n++;
        return n;
    };

    return run_fair(prod, cons, np, nc);
}

/* ========================================================================
 * Vyukov / rigtorp (bounded, single ring)
 * ======================================================================== */

static double bench_vyukov(int np, int nc) {
    rigtorp::MPMCQueue<uint64_t> q(CAPACITY);

    auto prod = [&](int id, std::atomic<bool> &run) {
        uint64_t v = (uint64_t)id << 48;
        while (run.load(std::memory_order_relaxed))
            q.try_push(v++);
    };

    auto cons = [&](int id, std::atomic<bool> &run) -> uint64_t {
        uint64_t v, n = 0;
        while (run.load(std::memory_order_relaxed))
            if (q.try_pop(v)) n++;
        return n;
    };

    return run_fair(prod, cons, np, nc);
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main() {
    int cfgs[][2] = {
        {1,1}, {2,2}, {4,4}, {8,8}, {16,16}, {4,1}, {4,8},
    };
    int ncfg = sizeof(cfgs) / sizeof(cfgs[0]);

    printf("# Fair C++ Benchmark — Items consumed/sec (Mops/s)\n\n");
    printf("Metric: consumer throughput only. Capacity: %d.\n", CAPACITY);
    printf("moodycamel is UNBOUNDED (uses malloc). Vyukov is bounded.\n\n");
    printf("| Config | moodycamel* | Vyukov |\n");
    printf("|--------|-------------|--------|\n");

    for (int c = 0; c < ncfg; c++) {
        int np = cfgs[c][0], nc = cfgs[c][1];
        fprintf(stderr, "  %dP/%dC: ", np, nc);

        double mc = bench_moodycamel(np, nc);
        double vy = bench_vyukov(np, nc);

        fprintf(stderr, "moodycamel=%.1f vyukov=%.1f\n", mc, vy);
        printf("| **%dP/%dC** | %.1f | %.1f |\n", np, nc, mc, vy);
    }

    printf("\n*moodycamel is unbounded (malloc when full). Not directly comparable to bounded designs.\n");
    return 0;
}
