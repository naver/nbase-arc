# Makefile
CC=gcc

# DEPENDENCIES
CAPI_DIR=../../api/arcci
OBJDIR=.obj64
CAPI_LIB=$(CAPI_DIR)/$(OBJDIR)/lib/libarcci.a

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
CFLAGS += -Wunreachable-code
CFLAGS += -Winit-self
CFLAGS += -Wmissing-field-initializers
CFLAGS += -Wno-unknown-pragmas
CFLAGS += -Wstrict-prototypes
CFLAGS += -Wundef
CFLAGS += -Wold-style-definition

DEFS=-D_REENTRANT -D_BSD_SOURCE -D_XOPEN_SOURCE
OPT=-O0 
DEBUG=-g -ggdb
INCLUDES = -I$(CAPI_DIR)
LIBS = $(CAPI_LIB) -lpthread -lm

CFLAGS += $(OPT) $(DEBUG) $(INCLUDES) $(DEFS) $(GCOV_FLAGS)

SOURCE=nbase-arc-cli.c linenoise.c cli_info_parser.c cli_err_arcci.c cli_cmd_arcci.c cli_cmd_cli.c cli_cmd_redis.c cli_cmd.c
EXEC=arc-cli
EXEC_ADMIN=arc-cli-admin

all: clean default

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage"

$(CAPI_LIB):
	cd $(CAPI_DIR) && $(MAKE)

default: $(CAPI_LIB)
	$(CC) $(CFLAGS) $(SOURCE) $(LIBS) -o $(EXEC)
	$(CC) $(CFLAGS) $(SOURCE) $(LIBS) -D_ADMIN_BUILD -o $(EXEC_ADMIN)

clean:
	rm -rf $(EXEC) $(EXEC_ADMIN)
	rm -rf *.o *.gcda *.gcno *.gcov
