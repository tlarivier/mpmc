/*
 * MPMC Test Suite Main
 * 
 * Includes all test files and runs them via utest.h
 * 
 * Note: mpmc_tf and mpmc_service tests are in rsos/ros2_bridge
 *       as those modules are ROS2-specific extensions.
 */

#include "test_common.h"

/* ============================================================================
 * MPMC Core Tests (mpmc.h)
 * ============================================================================ */
#include "mpmc/test_basic.c"
#include "mpmc/test_capacity.c"
#include "mpmc/test_invariants.c"
#include "mpmc/test_concurrent.c"
#include "mpmc/test_critical.c"
#include "mpmc/test_edge.c"
#include "mpmc/test_ordering.c"
#include "mpmc/test_security.c"
#include "mpmc/test_gaps.c"

/* ============================================================================
 * MPMC NUMA Tests (mpmc_numa.h)
 * ============================================================================ */
#include "mpmc/test_numa.c"

UTEST_MAIN()
