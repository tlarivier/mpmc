/*
 * bench_pmu.c — mseq throughput instrumented with hardware perf counters
 *               (LLC writes / misses, L1D misses, IPC). Calibrates the
 *               cost model with measured cache traffic, not just timing.
 *
 * Depends on perf-lite (sibling repo ../perf-lite); CMake / Makefile.linux
 * only build this target when that path exists. Linux only — macOS has
 * stubs that return zeros (no kernel perf interface). Needs root or
 * CAP_PERFMON to read the counters.
 *
 * Invariants:
 *  - Counters are armed AFTER the warmup window so the deltas reflect
 *    steady-state behaviour, not first-touch traffic.
 *  - One counter set per consumer thread; aggregated at exit. No
 *    counter read inside the hot path.
 *
 * Not allowed:
 *  - Run without root / CAP_PERFMON and trust the numbers — perf_event
 *    silently returns zero counts for unprivileged users on many kernels.
 *  - Add this binary to the default CI step; it requires elevated caps
 *    and an external repo, both of which are not always available.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>

#include "mpmc_seq.h"
#include "hwcounters.h"

#define CAPACITY 4096
#define ITEM_SIZE sizeof(uint64_t)
#define BENCH_SEC 2

/* ========================================================================
 * Benchmark core (same as bench_fair, but forked for PMU measurement)
 * ======================================================================== */

typedef struct {
    mseq_t *q; mseq_rpool_t *rp;
    _Atomic bool *run;
    _Atomic uint64_t *consumed;
    int id; int range;
} targ_t;

static void *mseq_prod(void *a) {
    targ_t *t = a; uint64_t v = 0;
    while (atomic_load(t->run)) {
        void *s = mseq_reserve(t->q, t->id, t->rp);
        if (s) { *(uint64_t *)s = v++; mseq_submit(t->q, t->id); }
    }
    return NULL;
}

static void *mseq_cons(void *a) {
    targ_t *t = a; uint64_t n = 0;
    while (atomic_load(t->run)) {
        mseq_claim_t c = mseq_claim(t->q, t->rp, t->id, t->range);
        if (c.data) { n += c.count; mseq_release(t->q, t->rp, t->id, &c); }
    }
    atomic_fetch_add(t->consumed, n);
    return NULL;
}

/*
 * Run benchmark in a child process so PMU counters capture everything.
 * Parent reads counters after child exits.
 */
static void run_pmu_bench(int np, int nc, int range) {
    uint32_t parts = np;
    if (parts & (parts-1)) { uint32_t v=parts; v--;v|=v>>1;v|=v>>2;v|=v>>4;v|=v>>8;v|=v>>16;parts=v+1; }
    uint32_t slots = CAPACITY / parts;
    if (slots < 8) slots = 8;

    printf("  %dP/%dC R=%d (parts=%u slots=%u):\n", np, nc, range, parts, slots);

    /* Fork: child runs the benchmark, parent attaches PMU */
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return; }

    if (pid == 0) {
        /* === CHILD: run the benchmark === */
        size_t qsz = mseq_buffer_size(parts, slots, ITEM_SIZE);
        void *qbuf = aligned_alloc(MSEQ_CACHE_LINE, qsz);
        mseq_t q; mseq_init(&q, qbuf, qsz, parts, slots, ITEM_SIZE);

        uint32_t ncons = nc > 0 ? nc : 1;
        size_t rsz = mseq_rpool_size(ncons);
        void *rbuf = aligned_alloc(MSEQ_CACHE_LINE, rsz);
        mseq_rpool_t rp; mseq_rpool_init(&rp, rbuf, rsz, ncons);

        _Atomic bool run = true;
        _Atomic uint64_t consumed = 0;

        int tot = np + nc;
        pthread_t *th = malloc(sizeof(pthread_t) * tot);
        targ_t *ar = malloc(sizeof(targ_t) * tot);
        for (int i = 0; i < np; i++) {
            ar[i] = (targ_t){&q, &rp, &run, &consumed, i, range};
            pthread_create(&th[i], NULL, mseq_prod, &ar[i]);
        }
        for (int i = 0; i < nc; i++) {
            ar[np+i] = (targ_t){&q, &rp, &run, &consumed, i, range};
            pthread_create(&th[np+i], NULL, mseq_cons, &ar[np+i]);
        }

        /* Warmup */
        struct timespec w = {0, 100000000}; nanosleep(&w, NULL);
        atomic_store(&consumed, 0);

        /* Signal parent: ready for PMU measurement */
        kill(getppid(), SIGUSR1);

        /* Run */
        struct timespec d = {BENCH_SEC, 0}; nanosleep(&d, NULL);
        atomic_store(&run, false);
        for (int i = 0; i < tot; i++) pthread_join(th[i], NULL);

        uint64_t cons = atomic_load(&consumed);
        double mops = cons / (double)BENCH_SEC / 1e6;

        /* Report throughput to stdout */
        printf("    throughput: %.1f Mops/s (%llu items consumed)\n",
               mops, (unsigned long long)cons);

        free(th); free(ar); free(qbuf); free(rbuf);
        _exit(0);
    }

    /* === PARENT: attach PMU counters to child === */
    /* Wait for child to signal readiness */
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    sigprocmask(SIG_BLOCK, &set, NULL);
    int sig;
    sigwait(&set, &sig);

    /* Open hardware counters on the child process */
    hwc_session_t *hwc = hwc_create(pid);
    if (hwc) {
        hwc_add(hwc, HWC_CPU_CYCLES);
        hwc_add(hwc, HWC_INSTRUCTIONS);
        hwc_add(hwc, HWC_CACHE_REFS);
        hwc_add(hwc, HWC_CACHE_MISSES);
        hwc_add_cache(hwc, HWC_CACHE_LLC_WRITE);
        hwc_add_cache(hwc, HWC_CACHE_LLC_WRITE_MISS);
        hwc_add_cache(hwc, HWC_CACHE_LLC_READ);
        hwc_add_cache(hwc, HWC_CACHE_LLC_READ_MISS);
        hwc_add_cache(hwc, HWC_CACHE_L1D_READ_MISS);
        hwc_add_cache(hwc, HWC_CACHE_L1D_WRITE_MISS);

        hwc_start(hwc);
    }

    /* Wait for child to finish */
    int status;
    waitpid(pid, &status, 0);

    /* Read and print PMU results */
    if (hwc) {
        hwc_stop(hwc);
        hwc_read(hwc);
        hwc_print(hwc, BENCH_SEC);
        hwc_destroy(hwc);
    } else {
        printf("    (PMU counters not available — Linux required)\n");
    }
    printf("\n");
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(void) {
    printf("# PMU-Instrumented MPMC Benchmark\n\n");
    printf("Platform: %s\n",
#if defined(__aarch64__)
        "aarch64"
#elif defined(__x86_64__)
        "x86_64"
#else
        "unknown"
#endif
    );
    printf("Capacity: %d total slots, Item: %zu bytes\n", CAPACITY, ITEM_SIZE);
    printf("Duration: %d sec per config\n\n", BENCH_SEC);

    struct { int np, nc, range; } cfgs[] = {
        {1, 1, 1},   {1, 1, 16},
        {4, 4, 1},   {4, 4, 16},
        {8, 8, 16},  {8, 8, 32},
        {4, 1, 16},  {4, 1, 32},
        {4, 8, 16},
    };
    int ncfg = sizeof(cfgs)/sizeof(cfgs[0]);

    for (int c = 0; c < ncfg; c++) {
        run_pmu_bench(cfgs[c].np, cfgs[c].nc, cfgs[c].range);
    }

    printf("## Key metrics for cost model calibration\n\n");
    printf("- LLC-write ≈ proxy for S→M transitions (exclusive cache-line acquisitions)\n");
    printf("- LLC-write / items_consumed = S→M transitions per item\n");
    printf("- Compare with model prediction: mseq R=N should have ~2/N S→M per item\n");
    printf("- IPC < 1.0 suggests pipeline stalls (likely from cache-line bouncing)\n");

    return 0;
}
