/*
 * evd_mseq.c — proof-of-concept: mseq as an event-bus backend with
 *              256-byte events (evd ev_t layout). 4 producer threads
 *              (hardware / display / network / security) feed 2
 *              consumers (handler + logger). 32 partitions × 128 slots,
 *              zero allocation.
 *
 * Demonstrates the producer-heavy regime (many sources, few sinks)
 * where mseq's per-partition SPMC design has its largest margin
 * over single-ring designs.
 *
 * Invariants:
 *  - Event payload is 256 bytes; partition slot_size is set accordingly.
 *  - Each producer thread owns exactly one partition id (the contract
 *    of mpmc_seq.h §4). Mapping is fixed at thread start.
 *
 * Not allowed:
 *  - Use this file as a benchmark — it is illustrative; throughput
 *    numbers from here are not the paper's measurements.
 *  - Mutate the ev_t layout without keeping it 256-byte-aligned —
 *    the layout matches the upstream evd format on purpose.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <unistd.h>

#include "mpmc_seq.h"

/* ============================================================================
 * Event format (compatible with evd ev_t, 256 bytes)
 * ============================================================================ */

#define EVD_MAX_PAYLOAD 192
#define EVD_MAX_SOURCE  16
#define EVD_MAX_NAME    32

typedef struct {
    uint32_t flags;
    uint32_t payload_len;
    uint64_t timestamp;
    char     source[EVD_MAX_SOURCE];
    char     name[EVD_MAX_NAME];
    char     payload[EVD_MAX_PAYLOAD];
} ev_t;

_Static_assert(sizeof(ev_t) == 256, "ev_t must be 256 bytes");

/* Partitions (named channels) */
enum {
    PART_HARDWARE = 0,
    PART_DISPLAY  = 1,
    PART_NETWORK  = 2,
    PART_SECURITY = 3,
    NUM_PARTITIONS = 4,
};

static const char *part_names[] = {
    "hardware", "display", "network", "security"
};

/* ============================================================================
 * Shared state
 * ============================================================================ */

#define SLOTS_PER_PART 1024
#define NUM_CONSUMERS  2
#define EVENTS_PER_PRODUCER 10000
#define BUF_SIZE (mseq_buffer_size(NUM_PARTITIONS, SLOTS_PER_PART, sizeof(ev_t)))

/* Static buffers — zero malloc */
/* Use dynamic alloc for the buffers (init-time only, not hot path).
 * In production, these would be stack/BSS allocated at known sizes. */
static void *queue_buf;
static void *rpool_buf;

static mseq_t queue;
static mseq_rpool_t rpool;

static _Atomic bool running = true;
static _Atomic uint64_t total_produced = 0;
static _Atomic uint64_t total_consumed = 0;
static _Atomic uint64_t consumer_events[NUM_CONSUMERS];

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* ============================================================================
 * Producers — simulate event sources
 * ============================================================================ */

static void emit(uint32_t partition, const char *source,
                 const char *name, const char *payload) {
    ev_t *ev = (ev_t *)mseq_reserve(&queue, partition, &rpool);
    if (!ev) return;  /* queue full, drop (like evd) */

    ev->flags = 0;
    ev->timestamp = now_ns();
    ev->payload_len = payload ? (uint32_t)strlen(payload) : 0;
    strncpy(ev->source, source, EVD_MAX_SOURCE - 1);
    strncpy(ev->name, name, EVD_MAX_NAME - 1);
    if (payload && ev->payload_len > 0)
        strncpy(ev->payload, payload, EVD_MAX_PAYLOAD - 1);

    mseq_submit(&queue, partition);
    atomic_fetch_add(&total_produced, 1);
}

static void *producer_hardware(void *arg) {
    (void)arg;
    for (int i = 0; i < EVENTS_PER_PRODUCER; i++) {
        char payload[32];
        snprintf(payload, sizeof(payload), "%d", 50 + (i % 100));
        emit(PART_HARDWARE, "volctl", "vol_changed", payload);
        /* no artificial delay — max throughput */
    }
    return NULL;
}

static void *producer_display(void *arg) {
    (void)arg;
    for (int i = 0; i < EVENTS_PER_PRODUCER; i++) {
        emit(PART_DISPLAY, "kanshi", "hotplug",
             i % 2 == 0 ? "connected" : "disconnected");
        if (i % 100 == 0) usleep(1);
    }
    return NULL;
}

static void *producer_network(void *arg) {
    (void)arg;
    for (int i = 0; i < EVENTS_PER_PRODUCER; i++) {
        emit(PART_NETWORK, "wifiselect", "connected", "HomeWiFi");
        if (i % 100 == 0) usleep(1);
    }
    return NULL;
}

static void *producer_security(void *arg) {
    (void)arg;
    for (int i = 0; i < EVENTS_PER_PRODUCER; i++) {
        emit(PART_SECURITY, "lock", "session_locked", "manual");
        if (i % 100 == 0) usleep(1);
    }
    return NULL;
}

/* ============================================================================
 * Consumers — simulate event handlers
 * ============================================================================ */

static void *consumer_handler(void *arg) {
    uint32_t cid = (uint32_t)(uintptr_t)arg;
    uint64_t count = 0;

    while (atomic_load(&running)) {
        mseq_claim_t c = mseq_claim(&queue, &rpool, cid, 8);
        if (!c.data) { usleep(100); continue; }

        for (uint32_t i = 0; i < c.count; i++) {
            ev_t *ev = (ev_t *)mseq_claim_item(&queue, &c, i);
            (void)ev;  /* "process" the event */
            count++;
        }

        mseq_release(&queue, &rpool, cid, &c);
        atomic_fetch_add(&total_consumed, c.count);
    }

    atomic_store(&consumer_events[cid], count);
    return NULL;
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("evd_mseq — Event bus proof-of-concept\n");
    printf("  Partitions: %d (%s, %s, %s, %s)\n", NUM_PARTITIONS,
           part_names[0], part_names[1], part_names[2], part_names[3]);
    printf("  Slots/partition: %d, Event size: %zu bytes\n",
           SLOTS_PER_PART, sizeof(ev_t));
    printf("  Producers: %d, Consumers: %d\n", NUM_PARTITIONS, NUM_CONSUMERS);
    printf("  Events/producer: %d, Total: %d\n",
           EVENTS_PER_PRODUCER, NUM_PARTITIONS * EVENTS_PER_PRODUCER);
    size_t qsz = mseq_buffer_size(NUM_PARTITIONS, SLOTS_PER_PART, sizeof(ev_t));
    size_t rsz = mseq_rpool_size(NUM_CONSUMERS);
    printf("  Allocation: zero (static buffers, %zu + %zu bytes)\n",
           qsz, rsz);
    printf("\n");

    /* Init */
    queue_buf = aligned_alloc(MSEQ_CACHE_LINE, qsz);
    rpool_buf = aligned_alloc(MSEQ_CACHE_LINE, rsz);
    if (!queue_buf || !rpool_buf) { fprintf(stderr, "alloc failed\n"); return 1; }

    printf("  Queue buffer: %zu bytes, Rpool buffer: %zu bytes\n\n", qsz, rsz);

    if (mseq_init(&queue, queue_buf, qsz,
                  NUM_PARTITIONS, SLOTS_PER_PART, sizeof(ev_t)) != 0) {
        fprintf(stderr, "mseq_init failed\n");
        return 1;
    }
    if (mseq_rpool_init(&rpool, rpool_buf, rsz, NUM_CONSUMERS) != 0) {
        fprintf(stderr, "mseq_rpool_init failed\n");
        return 1;
    }

    atomic_store(&total_produced, 0);
    atomic_store(&total_consumed, 0);
    for (int i = 0; i < NUM_CONSUMERS; i++)
        atomic_store(&consumer_events[i], 0);

    uint64_t t0 = now_ns();

    /* Start consumers */
    pthread_t cons[NUM_CONSUMERS];
    for (int i = 0; i < NUM_CONSUMERS; i++)
        pthread_create(&cons[i], NULL, consumer_handler, (void *)(uintptr_t)i);

    /* Start producers */
    pthread_t prods[NUM_PARTITIONS];
    pthread_create(&prods[0], NULL, producer_hardware, NULL);
    pthread_create(&prods[1], NULL, producer_display, NULL);
    pthread_create(&prods[2], NULL, producer_network, NULL);
    pthread_create(&prods[3], NULL, producer_security, NULL);

    /* Wait for producers */
    for (int i = 0; i < NUM_PARTITIONS; i++)
        pthread_join(prods[i], NULL);

    /* Let consumers drain remaining events */
    usleep(100000);  /* 100ms grace period */
    atomic_store(&running, false);
    for (int i = 0; i < NUM_CONSUMERS; i++)
        pthread_join(cons[i], NULL);

    uint64_t t1 = now_ns();
    double elapsed = (t1 - t0) / 1e9;
    uint64_t produced = atomic_load(&total_produced);
    uint64_t consumed = atomic_load(&total_consumed);

    printf("Results:\n");
    printf("  Produced: %llu events\n", (unsigned long long)produced);
    printf("  Consumed: %llu events\n", (unsigned long long)consumed);
    for (int i = 0; i < NUM_CONSUMERS; i++)
        printf("  Consumer %d: %llu events\n", i,
               (unsigned long long)atomic_load(&consumer_events[i]));
    printf("  Elapsed: %.3f s\n", elapsed);
    printf("  Throughput: %.1f Kevents/s\n", consumed / elapsed / 1e3);
    printf("  Lost: %llu events (%.1f%%)\n",
           (unsigned long long)(produced > consumed ? produced - consumed : 0),
           produced > 0 ? 100.0 * (produced - consumed) / produced : 0.0);
    printf("\n");

    if (consumed == produced) {
        printf("  STATUS: ALL EVENTS DELIVERED (zero loss)\n");
    } else {
        printf("  STATUS: %llu events lost (backpressure)\n",
               (unsigned long long)(produced - consumed));
    }

    return 0;
}
