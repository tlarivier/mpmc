/*
 * MPMC NUMA-Aware Extension
 * 
 * Provides NUMA-optimized allocation and consumption strategies.
 * Requires libnuma on Linux. Falls back to standard mpmc on other platforms.
 * 
 * Design: One mpmc_t per NUMA node, with local-first scanning.
 * 
 * Key optimizations:
 * 1. Each node has its own mpmc_t allocated locally
 * 2. Consumers scan local node first, then remote
 * 3. Producers use their node-local queue
 */

#ifndef MPMC_NUMA_H
#define MPMC_NUMA_H

#include "mpmc.h"
#include <stdio.h>

#ifdef __linux__
#include <numa.h>
#include <numaif.h>
#include <sched.h>
#define MPMC_NUMA_AVAILABLE 1
#else
#define MPMC_NUMA_AVAILABLE 0
#endif

/* ============================================================================
 * Configuration
 * ============================================================================ */

#define MPMC_NUMA_MAX_NODES 8

typedef struct {
    int num_nodes;                          /* Number of NUMA nodes */
    uint32_t partitions_per_node;           /* Partitions per node */
    uint32_t slots_per_partition;           /* Slots per partition */
    uint32_t slot_size;                     /* Bytes per slot */
} mpmc_numa_config_t;

/* ============================================================================
 * NUMA-aware queue structure
 * ============================================================================ */

typedef struct {
    /* One queue per NUMA node */
    mpmc_t nodes[MPMC_NUMA_MAX_NODES];
    
    /* NUMA topology */
    int num_nodes;
    uint32_t partitions_per_node;
    uint32_t total_partitions;
    
    /* Per-node ready hint (set when node has data) */
    _Atomic(uint64_t) node_ready_mask;
    
} mpmc_numa_t;

/* ============================================================================
 * NUMA Utilities
 * ============================================================================ */

static inline int mpmc_numa_get_node_count(void) {
#if MPMC_NUMA_AVAILABLE
    if (numa_available() < 0) return 1;
    return numa_num_configured_nodes();
#else
    return 1;
#endif
}

static inline int mpmc_numa_get_current_node(void) {
#if MPMC_NUMA_AVAILABLE
    if (numa_available() < 0) return 0;
    int cpu = sched_getcpu();
    if (cpu < 0) return 0;
    return numa_node_of_cpu(cpu);
#else
    return 0;
#endif
}

static inline int mpmc_numa_bind_thread(int node) {
#if MPMC_NUMA_AVAILABLE
    if (numa_available() >= 0 && node >= 0) {
        struct bitmask *mask = numa_allocate_nodemask();
        numa_bitmask_setbit(mask, node);
        numa_bind(mask);
        numa_free_nodemask(mask);
        return 0;
    }
#endif
    (void)node;
    return -1;
}

/* ============================================================================
 * Initialization
 * ============================================================================ */

/*
 * Initialize NUMA-aware MPMC queue.
 * Creates one mpmc_t per NUMA node.
 */
static inline int mpmc_numa_init(mpmc_numa_t *q, mpmc_numa_config_t *config) {
    memset(q, 0, sizeof(*q));
    
    if (config->num_nodes > MPMC_NUMA_MAX_NODES) return -1;
    if (config->partitions_per_node == 0) return -1;
    
    q->num_nodes            = config->num_nodes;
    q->partitions_per_node  = config->partitions_per_node;
    q->total_partitions     = config->num_nodes * config->partitions_per_node;
    atomic_store(&q->node_ready_mask, 0);
    
    for (int node = 0; node < config->num_nodes; node++) {
#if MPMC_NUMA_AVAILABLE
        if (numa_available() >= 0) {
            struct bitmask *mask = numa_allocate_nodemask();
            numa_bitmask_setbit(mask, node);
            numa_set_membind(mask);
            numa_free_nodemask(mask);
        }
#endif
        
        int ret = mpmc_init(&q->nodes[node], 
                           config->partitions_per_node,
                           config->slots_per_partition,
                           config->slot_size);
        
#if MPMC_NUMA_AVAILABLE
        /* Reset memory binding */
        if (numa_available() >= 0) {
            numa_set_membind(numa_all_nodes_ptr);
        }
#endif
        
        if (ret != 0) {
            for (int i = 0; i < node; i++) {
                mpmc_destroy(&q->nodes[i]);
            }
            return -1;
        }
    }
    
    return 0;
}

static inline void mpmc_numa_destroy(mpmc_numa_t *q) {
    for (int node = 0; node < q->num_nodes; node++) {
        mpmc_destroy(&q->nodes[node]);
    }
    memset(q, 0, sizeof(*q));
}

/* ============================================================================
 * Producer API
 * ============================================================================ */

static inline void* mpmc_numa_reserve(mpmc_numa_t *q, int node, uint32_t local_part) {
    if (node < 0 || node >= q->num_nodes) return NULL;
    if (local_part >= q->partitions_per_node) return NULL;
    return mpmc_reserve(&q->nodes[node], local_part);
}

static inline void mpmc_numa_submit(mpmc_numa_t *q, int node, uint32_t local_part) {
    if (node < 0 || node >= q->num_nodes) return;
    mpmc_submit(&q->nodes[node], local_part);
    
    /* Set node ready hint */
    atomic_fetch_or(&q->node_ready_mask, (1ULL << node));
}

static inline void mpmc_numa_discard(mpmc_numa_t *q, int node, uint32_t local_part) {
    if (node < 0 || node >= q->num_nodes) return;
    mpmc_discard(&q->nodes[node], local_part);
}

/* ============================================================================
 * Consumer API 
 * ============================================================================ */

typedef struct {
    void    *data;
    int      node;
    uint32_t part;
    uint32_t slot;
} mpmc_numa_item_t;

static inline mpmc_numa_item_t mpmc_numa_claim_local_first(mpmc_numa_t *q) {
    mpmc_numa_item_t result = {0};
    int my_node = mpmc_numa_get_current_node();
    
    if (my_node >= q->num_nodes) my_node = 0;
    
    /* 1: Try local node first */
    mpmc_item_t item = mpmc_claim(&q->nodes[my_node]);
    if (item.data) {
        result.data = item.data;
        result.node = my_node;
        result.part = item.part;
        result.slot = item.slot;
        return result;
    }
    
    /* 2: Try remote nodes */
    for (int node = 0; node < q->num_nodes; node++) {
        if (node == my_node) continue;
        
        uint64_t mask = atomic_load_explicit(&q->node_ready_mask, memory_order_relaxed);
        if (!(mask & (1ULL << node))) continue;
        
        item = mpmc_claim(&q->nodes[node]);
        if (item.data) {
            result.data = item.data;
            result.node = node;
            result.part = item.part;
            result.slot = item.slot;
            return result;
        } else {
            atomic_fetch_and(&q->node_ready_mask, ~(1ULL << node));
        }
    }
    
    return result;  
}

static inline mpmc_numa_item_t mpmc_numa_claim(mpmc_numa_t *q) {
    mpmc_numa_item_t result = {0};
    uint64_t mask = atomic_load_explicit(&q->node_ready_mask, memory_order_relaxed);
    
    for (int node = 0; node < q->num_nodes; node++) {
        if (node < 64 && !(mask & (1ULL << node))) continue;
        
        mpmc_item_t item = mpmc_claim(&q->nodes[node]);
        if (item.data) {
            result.data = item.data;
            result.node = node;
            result.part = item.part;
            result.slot = item.slot;
            return result;
        } else if (node < 64) {
            atomic_fetch_and(&q->node_ready_mask, ~(1ULL << node));
        }
    }
    
    return result;
}

/*
 * Release a claimed item back to its node.
 * 
 * NOTE: Does NOT clear node_ready_mask here to avoid O(P×S) scan.
 * The hint is cleared lazily by claim functions when they find empty nodes.
 * This is the same strategy used by the base mpmc_claim() with ready_mask.
 */
static inline void mpmc_numa_release(mpmc_numa_t *q, mpmc_numa_item_t *item) {
    if (!item->data || item->node < 0 || item->node >= q->num_nodes) return;
    
    int node = item->node;
    mpmc_item_t base_item = {
        .data = item->data,
        .part = item->part,
        .slot = item->slot
    };
    mpmc_release(&q->nodes[node], &base_item);
    item->data = NULL;
}

/* ============================================================================
 * Batch API 
 * ============================================================================ */

#define MPMC_NUMA_MAX_BATCH 64

typedef struct {
    mpmc_numa_item_t items[MPMC_NUMA_MAX_BATCH];
    uint32_t count;
} mpmc_numa_batch_t;

static inline int mpmc_numa_claim_batch_local(mpmc_numa_t *q, 
                                              mpmc_numa_batch_t *batch, 
                                              uint32_t max) {
    int my_node = mpmc_numa_get_current_node();
    if (my_node >= q->num_nodes) my_node = 0;
    
    batch->count = 0;
    if (max > MPMC_NUMA_MAX_BATCH) max = MPMC_NUMA_MAX_BATCH;
    
    mpmc_batch_t local_batch;
    uint32_t count = (uint32_t)mpmc_claim_batch(&q->nodes[my_node], &local_batch, max);
    
    for (uint32_t i = 0; i < count; i++) {
        batch->items[i].data = local_batch.items[i].data;
        batch->items[i].node = my_node;
        batch->items[i].part = local_batch.items[i].part;
        batch->items[i].slot = local_batch.items[i].slot;
    }
    batch->count = count;
    
    return count;
}

static inline void mpmc_numa_release_batch(mpmc_numa_t *q, mpmc_numa_batch_t *batch) {
    for (uint32_t i = 0; i < batch->count; i++) {
        mpmc_numa_release(q, &batch->items[i]);
    }
    batch->count = 0;
}

/* ============================================================================
 * Utilities
 * ============================================================================ */

static inline int mpmc_numa_has_data(mpmc_numa_t *q) {
    for (int node = 0; node < q->num_nodes; node++) {
        if (mpmc_has_data(&q->nodes[node])) return 1;
    }
    return 0;
}

static inline mpmc_numa_item_t mpmc_numa_claim_from(mpmc_numa_t *q, int preferred_node) {
    mpmc_numa_item_t result = {0};
    
    if (preferred_node < 0 || preferred_node >= q->num_nodes) {
        preferred_node = 0;
    }
    
    mpmc_item_t item = mpmc_claim(&q->nodes[preferred_node]);
    if (item.data) {
        result.data = item.data;
        result.node = preferred_node;
        result.part = item.part;
        result.slot = item.slot;
        return result;
    }
    
    for (int node = 0; node < q->num_nodes; node++) {
        if (node == preferred_node) continue;
        
        item = mpmc_claim(&q->nodes[node]);
        if (item.data) {
            result.data = item.data;
            result.node = node;
            result.part = item.part;
            result.slot = item.slot;
            return result;
        }
    }
    
    return result;
}

static inline void mpmc_numa_print_info(mpmc_numa_t *q) {
    printf("MPMC NUMA Queue:\n");
    printf("  Nodes:            %d\n", q->num_nodes);
    printf("  Partitions/node:  %u\n", q->partitions_per_node);
    printf("  Total partitions: %u\n", q->total_partitions);
    printf("  Slots/partition:  %u\n", q->nodes[0].slots_per_part);
    printf("  Slot size: %u bytes\n",  q->nodes[0].slot_size);
    
#if MPMC_NUMA_AVAILABLE
    if (numa_available() >= 0) {
        printf("  NUMA: enabled (libnuma)\n");
        printf("  Current node: %d\n", mpmc_numa_get_current_node());
    } else {
        printf("  NUMA: disabled (numa_available failed)\n");
    }
#else
    printf("  NUMA: not available (non-Linux or no libnuma)\n");
#endif
}

#endif /* MPMC_NUMA_H */
