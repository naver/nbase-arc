# Makefile
CC=gcc

# DEPENDENCIES
DEPS=deps
JEMALLOC=../redis-3.2.9/deps/jemalloc

# SETTINGS
CFLAGS=-std=c99
CFLAGS += -pedantic
CFLAGS += -rdynamic
CFLAGS += -Wall
CFLAGS += -Wextra
CFLAGS += -Werror 
CFLAGS += -Wpointer-arith
CFLAGS += -Wcast-align
CFLAGS += -Wwrite-strings
CFLAGS += -Wswitch-default
CFLAGS += -Winit-self
CFLAGS += -Wmissing-field-initializers
CFLAGS += -Wno-unknown-pragmas
CFLAGS += -Wstrict-prototypes
CFLAGS += -Wold-style-definition

DEFS=-D_REENTRANT -D_GNU_SOURCE -DUSE_JEMALLOC
OPT=-O3
DEBUG=-g -ggdb
INCLUDES=-I$(DEPS) -I$(JEMALLOC)/include

CFLAGS += $(OPT) $(DEBUG) $(INCLUDES) $(DEFS) $(GCOV_FLAGS)
LIBS=$(JEMALLOC)/lib/libjemalloc.a -lpthread

DEPS_SOURCE=ae.c zmalloc.c dict.c util.c anet.c sds.c base64.c
GATEWAY_SOURCE=gw_main.c gw_config.c gw_worker.c gw_redis_pool.c gw_client.c gw_cmd_mgr.c gw_cmd_redis.c gw_cmd_admin.c gw_cmd_stat.c gw_redis_parser.c gw_stream_buf.c gw_util.c gw_async_chan.c gw_log.c
SOURCE=$(GATEWAY_SOURCE) $(addprefix $(DEPS)/,$(DEPS_SOURCE))
OBJECT=$(SOURCE:.c=.o)
GCOV_DATA=$(SOURCE:.c=.gcno) $(SOURCE:.c=.gcda)

all: redis-gateway

redis-gateway: $(OBJECT)
	$(LINK.c) $(OBJECT) -o $@ $(LIBS)

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage"

test:

clean:
	$(RM) redis-gateway $(OBJECT) $(GCOV_DATA)
