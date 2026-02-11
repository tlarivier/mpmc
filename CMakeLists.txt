cmake_minimum_required(VERSION 3.16)
project(mpmc_queue C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
add_compile_options(-Wall -Wextra -O3 -march=native)

# ============================================================================
# Sanitizer Options
# ============================================================================
option(MPMC_SANITIZE "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(MPMC_TSAN "Enable ThreadSanitizer (incompatible with ASAN)" OFF)
option(MPMC_WERROR "Treat warnings as errors" OFF)

if(MPMC_SANITIZE AND MPMC_TSAN)
    message(FATAL_ERROR "Cannot enable both ASAN and TSAN simultaneously")
endif()

if(MPMC_SANITIZE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address,undefined)
    message(STATUS "Sanitizers: ASan + UBSan enabled")
endif()

if(MPMC_TSAN)
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=thread)
    message(STATUS "Sanitizers: TSan enabled")
endif()

if(MPMC_WERROR)
    add_compile_options(-Werror)
    message(STATUS "Warnings treated as errors")
endif()

set(VENDOR ${CMAKE_SOURCE_DIR}/vendor)
include_directories(include ${VENDOR}/test/utest)

# ============================================================================
# Tests
# ============================================================================
add_executable(mpmc_test src/tests/main.c)
target_include_directories(mpmc_test PRIVATE src/tests)
target_link_libraries(mpmc_test pthread)

# ============================================================================
# Examples
# ============================================================================
add_executable(example_basic src/examples/basic_usage.c)
target_link_libraries(example_basic pthread)

add_executable(example_pipeline src/examples/robot_pipeline.c)
target_link_libraries(example_pipeline pthread m)

# NUMA example (Linux only)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_library(NUMA_LIB numa)
    if(NUMA_LIB)
        add_executable(example_numa src/examples/numa_example.c)
        target_link_libraries(example_numa pthread ${NUMA_LIB})
        message(STATUS "NUMA support: enabled")
    else()
        message(STATUS "NUMA support: disabled (libnuma not found)")
    endif()
else()
    message(STATUS "NUMA support: disabled (non-Linux)")
endif()

# ============================================================================
# Benchmark (MPMC + MPMC-NUMA + Folly + CK + Moodycamel + Vyukov)
# ============================================================================
set(CK_LIB ${VENDOR}/bench/ck/src/libck.a)

# Find Folly (installed via Homebrew)
find_package(folly CONFIG QUIET)
if(NOT folly_FOUND)
    # Try pkg-config as fallback
    find_package(PkgConfig)
    if(PkgConfig_FOUND)
        pkg_check_modules(FOLLY folly)
    endif()
endif()

# C sources
add_library(bench_c OBJECT
    src/bench/benchmark.c
    src/bench/queue_mpmc.c
    src/bench/queue_mpmc_numa.c
    src/bench/queue_ck.c
)
target_include_directories(bench_c PRIVATE 
    src/bench 
    ${VENDOR}/bench/ck/include
)

# C++ sources (Moodycamel + Vyukov) - compiled without Folly includes
add_library(bench_cxx OBJECT 
    src/bench/queue_moodycamel.cpp
    src/bench/queue_vyukov.cpp
)
target_include_directories(bench_cxx PRIVATE 
    src/bench
    ${VENDOR}/bench/moodycamel
    ${VENDOR}/bench/vyukov/include
)

# Folly compiled separately - Linux only due to macOS header conflicts
add_library(bench_folly OBJECT src/bench/queue_folly.cpp)
target_include_directories(bench_folly PRIVATE src/bench)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND folly_FOUND)
    target_compile_definitions(bench_folly PRIVATE FOLLY_ENABLED)
    target_link_libraries(bench_folly PRIVATE Folly::folly)
    message(STATUS "Folly benchmark: enabled")
else()
    message(STATUS "Folly benchmark: disabled (stub only)")
endif()

# Link benchmark
add_executable(mpmc_bench 
    $<TARGET_OBJECTS:bench_c> 
    $<TARGET_OBJECTS:bench_cxx>
    $<TARGET_OBJECTS:bench_folly>
)
target_link_libraries(mpmc_bench ${CK_LIB} pthread)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(folly_FOUND)
        target_link_libraries(mpmc_bench Folly::folly)
    endif()
    if(NUMA_LIB)
        target_link_libraries(mpmc_bench ${NUMA_LIB})
    endif()
endif()
