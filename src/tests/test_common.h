/*
 * MPMC Test Common Header
 */

#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include "utest.h"
#include "mpmc.h"
#include <pthread.h>
#include <time.h>
#include <string.h>
#include <sched.h>

/* TSAN annotations for lock-free synchronization */
#if defined(__SANITIZE_THREAD__) || defined(__has_feature)
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define TSAN_ENABLED 1
#endif
#endif
#endif

#ifdef TSAN_ENABLED
#define TSAN_ANNOTATE_HAPPENS_BEFORE(addr) \
    AnnotateHappensBefore(__FILE__, __LINE__, (void *)(addr))
#define TSAN_ANNOTATE_HAPPENS_AFTER(addr) \
    AnnotateHappensAfter(__FILE__, __LINE__, (void *)(addr))
void AnnotateHappensBefore(const char *f, int l, void *addr);
void AnnotateHappensAfter(const char *f, int l, void *addr);
#else
#define TSAN_ANNOTATE_HAPPENS_BEFORE(addr) ((void)0)
#define TSAN_ANNOTATE_HAPPENS_AFTER(addr) ((void)0)
#endif

/* Test configuration */
#define TEST_PARTITIONS 4
#define TEST_SLOTS      64
#define TEST_SLOT_SIZE  64

#endif /* TEST_COMMON_H */
