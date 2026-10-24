CC  ?= cc
CXX ?= c++

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifneq ($(filter aarch64 arm64,$(UNAME_M)),)
  CACHE_LINE ?= 128
else
  CACHE_LINE ?= 64
endif
