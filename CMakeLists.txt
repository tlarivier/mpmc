cmake_minimum_required(VERSION 3.16)
project(mseq C)

set(CMAKE_C_STANDARD 11)
add_compile_options(-Wall -Wextra -O3 -march=native)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    add_definitions(-DMSEQ_CACHE_LINE=128)
else()
    add_definitions(-DMSEQ_CACHE_LINE=64)
endif()

set(VENDOR ${CMAKE_SOURCE_DIR}/vendor)
include_directories(include ${VENDOR}/test/utest)

add_executable(mseq_test src/tests/main.c)
target_link_libraries(mseq_test pthread)

add_executable(bench_compare src/bench/bench_compare.c)
target_include_directories(bench_compare PRIVATE ${VENDOR}/bench/ck/include)
target_link_libraries(bench_compare pthread)

add_executable(bench_mesi src/bench/bench_mesi.c)
target_include_directories(bench_mesi PRIVATE include)
target_link_libraries(bench_mesi pthread)

add_executable(bench_latency src/bench/bench_latency.c)
target_link_libraries(bench_latency pthread)

if(EXISTS ${CMAKE_SOURCE_DIR}/../perf-lite/src/hwc/hwcounters.c)
    add_executable(bench_pmu
        src/bench/bench_pmu.c
        ${CMAKE_SOURCE_DIR}/../perf-lite/src/hwc/hwcounters.c)
    target_include_directories(bench_pmu PRIVATE
        ${CMAKE_SOURCE_DIR}/../perf-lite/include)
    target_link_libraries(bench_pmu pthread)
    message(STATUS "PMU benchmark: enabled (perf-lite found)")
else()
    message(STATUS "PMU benchmark: disabled (perf-lite not found at ../perf-lite)")
endif()

add_executable(bench_fair src/bench/bench_fair.c)
target_include_directories(bench_fair PRIVATE
    ${VENDOR}/bench/ck/include ${VENDOR}/bench/scq ${VENDOR}/bench/scq/lf)
target_link_libraries(bench_fair pthread)

add_executable(bench_scq src/bench/bench_scq.c)
target_include_directories(bench_scq PRIVATE ${VENDOR}/bench/scq ${VENDOR}/bench/scq/lf)
target_link_libraries(bench_scq pthread)

option(BUILD_CXX_BENCH "Build C++ reference benchmarks (requires moodycamel + rigtorp headers)" OFF)
if(BUILD_CXX_BENCH)
    enable_language(CXX)
    set(CMAKE_CXX_STANDARD 17)
    if(EXISTS ${VENDOR}/bench/vyukov/include/rigtorp/MPMCQueue.h)
        add_executable(bench_fair_cxx src/bench/bench_fair_cxx.cpp)
        target_include_directories(bench_fair_cxx PRIVATE
            include ${VENDOR}/bench/moodycamel ${VENDOR}/bench/vyukov/include)
        target_link_libraries(bench_fair_cxx pthread)
        message(STATUS "C++ benchmark: enabled")
    else()
        message(STATUS "C++ benchmark: disabled (rigtorp/MPMCQueue.h not found in ${VENDOR}/bench/vyukov)")
    endif()
else()
    message(STATUS "C++ benchmark: disabled (set -DBUILD_CXX_BENCH=ON to enable)")
endif()

option(BUILD_TSAN "Build tests with ThreadSanitizer" OFF)
if(BUILD_TSAN)
    add_executable(mseq_test_tsan src/tests/main.c)
    target_link_libraries(mseq_test_tsan pthread)
    target_compile_options(mseq_test_tsan PRIVATE -fsanitize=thread -g -O1)
    target_link_options(mseq_test_tsan PRIVATE -fsanitize=thread)
    message(STATUS "TSAN test: enabled")
endif()
