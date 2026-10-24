PROJECT_NAME := mseq

BUILD_DIR := build
OBJ_DIR   := $(BUILD_DIR)/obj
BIN_DIR   := $(BUILD_DIR)/bin

VENDOR    := vendor
PERF_LITE := ../perf-lite

.DEFAULT_GOAL := all
.SUFFIXES:

include rules/toolchain.mk
include rules/functions.mk

CPPFLAGS    := -Iinclude -D_GNU_SOURCE -DMSEQ_CACHE_LINE=$(CACHE_LINE)
CFLAGS      := -std=c11 -O3 -march=native -Wall -Wextra
CXXFLAGS    := -std=c++17 -O3 -march=native -Wall -Wextra
TSAN_CFLAGS := -std=c11 -O1 -g -fsanitize=thread -Wall -Wextra
LDLIBS      := -lpthread
LINK         = $(CC)

TEST_SRC   := src/tests/main.c
BENCH_SRCS := $(addprefix src/bench/,bench_fair.c bench_compare.c bench_scq.c \
                bench_announce.c bench_latency.c bench_mesi.c)
EX_SRCS    := src/examples/evd_mseq.c
PMU_SRCS   := src/bench/bench_pmu.c
CXX_SRCS   := src/bench/bench_fair_cxx.cpp
PMU_HWC    := $(PERF_LITE)/src/hwc/hwcounters.c
PMU_HWC_O  := $(OBJ_DIR)/perf-lite/hwcounters.o

TEST_BIN  := $(BIN_DIR)/mseq_test
TSAN_BIN  := $(BIN_DIR)/mseq_test_tsan
BENCHES   := $(addprefix $(BIN_DIR)/,$(notdir $(basename $(BENCH_SRCS))))
EXAMPLES  := $(addprefix $(BIN_DIR)/,$(notdir $(basename $(EX_SRCS))))
PMU_BIN   := $(BIN_DIR)/bench_pmu
CXX_BIN   := $(BIN_DIR)/bench_fair_cxx

ALL_BINS := $(TEST_BIN) $(BENCHES) $(EXAMPLES)
ifneq ($(wildcard $(PMU_HWC)),)
  ALL_BINS += $(PMU_BIN)
endif

include rules/compile.mk
include rules/link.mk
include rules/help.mk

$(call c_objs,src/bench/bench_fair.c):    CPPFLAGS += -I$(VENDOR)/bench/ck/include -I$(VENDOR)/bench/scq -I$(VENDOR)/bench/scq/lf
$(call c_objs,src/bench/bench_compare.c): CPPFLAGS += -I$(VENDOR)/bench/ck/include
$(call c_objs,src/bench/bench_scq.c):     CPPFLAGS += -I$(VENDOR)/bench/scq -I$(VENDOR)/bench/scq/lf
$(call c_objs,$(TEST_SRC)) $(call tsan_objs,$(TEST_SRC)): CPPFLAGS += -I$(VENDOR)/test/utest
$(call c_objs,$(PMU_SRCS)) $(PMU_HWC_O):  CPPFLAGS += -I$(PERF_LITE)/include
$(call cxx_objs,$(CXX_SRCS)):             CPPFLAGS += -I$(VENDOR)/bench/moodycamel -I$(VENDOR)/bench/vyukov/include

$(PMU_HWC_O): $(PMU_HWC)
	@mkdir -p $(dir $@)
	@echo "  CC      $<"
	@$(CC) $(CFLAGS) $(CPPFLAGS) -MMD -MP -c $< -o $@

$(TEST_BIN): $(call c_objs,$(TEST_SRC))
$(TSAN_BIN): $(call tsan_objs,$(TEST_SRC))
$(TSAN_BIN): LDFLAGS := -fsanitize=thread
$(BENCHES):  $(BIN_DIR)/%: $(OBJ_DIR)/src/bench/%.o
$(EXAMPLES): $(BIN_DIR)/%: $(OBJ_DIR)/src/examples/%.o
$(PMU_BIN): $(call c_objs,$(PMU_SRCS)) $(PMU_HWC_O)
$(CXX_BIN): $(call cxx_objs,$(CXX_SRCS))
$(CXX_BIN): LINK = $(CXX)

FLAGS_c    := $(CC) $(CPPFLAGS) $(CFLAGS)
FLAGS_tsan := $(CC) $(CPPFLAGS) $(TSAN_CFLAGS)
FLAGS_cxx  := $(CXX) $(CPPFLAGS) $(CXXFLAGS)

$(BUILD_DIR)/%.flags: FORCE
	@mkdir -p $(@D)
	@printf '%s' '$(FLAGS_$*)' | cmp -s - $@ 2>/dev/null || printf '%s' '$(FLAGS_$*)' > $@
FORCE: ;

C_OBJS := $(call c_objs,$(TEST_SRC) $(BENCH_SRCS) $(EX_SRCS) $(PMU_SRCS)) $(PMU_HWC_O)
$(C_OBJS):                        $(BUILD_DIR)/c.flags
$(call tsan_objs,$(TEST_SRC)):    $(BUILD_DIR)/tsan.flags
$(call cxx_objs,$(CXX_SRCS)):     $(BUILD_DIR)/cxx.flags

all: $(ALL_BINS) ## Build tests, benchmarks and examples (default)

test: $(TEST_BIN) ## Build and run the test suite
	@$(TEST_BIN)

tsan: $(TSAN_BIN) ## Build and run the test suite under ThreadSanitizer
	@$(TSAN_BIN)

bench: $(BIN_DIR)/bench_fair ## Run bench_fair (README numbers)
	@$(BIN_DIR)/bench_fair

pmu: $(PMU_BIN) ## Run bench_pmu (Linux, needs ../perf-lite and CAP_PERFMON)
	@$(PMU_BIN)

cxx: $(CXX_BIN) ## Build bench_fair_cxx (moodycamel, rigtorp)

info: ## Show build configuration
	@echo "$(PROJECT_NAME)"
	@echo "CC         : $(CC)"
	@echo "Cache line : $(CACHE_LINE)"
	@echo "Binaries   : $(notdir $(ALL_BINS))"

clean: ## Remove the build directory
	@echo "Cleaning $(BUILD_DIR)..."
	@rm -rf $(BUILD_DIR)

re: ## Rebuild from scratch (clean + all)
	@$(MAKE) --no-print-directory clean
	@$(MAKE) --no-print-directory all

.PHONY: all test tsan bench pmu cxx info clean re FORCE
.SECONDARY:

DEP_OBJS := $(C_OBJS) $(call tsan_objs,$(TEST_SRC)) $(call cxx_objs,$(CXX_SRCS))
-include $(DEP_OBJS:.o=.d)
