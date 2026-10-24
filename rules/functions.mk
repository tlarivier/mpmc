c_objs    = $(patsubst %.c,$(OBJ_DIR)/%.o,$(1))
cxx_objs  = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(1))
tsan_objs = $(patsubst %.c,$(OBJ_DIR)/tsan/%.o,$(1))
