# Makefile

CC = gcc

DEFS = -D_GNU_SOURCE -D_REENTRANT
OPT = -O0
DEBUG = -g -ggdb
INCLUDES = -I../../smr -I../../replicator

CFLAGS = -std=c99 -pedantic -Wall $(INCLUDES) $(DEFS) $(OPT) $(DEBUG) $(GCOV_FLAGS)
LINKS = -lpthread ../../smr/libsmr.a -lrt
OBJECT = ../../replicator/part_filter.o ../../replicator/slowlog.o
TESTS = test-stream test-filter test-rbtree test-dlist test-log test-slowlog test-memdev
PERFS =

all: $(OBJECT) $(TESTS) $(PERFS)

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage -DSFI_ENABLED"

# executables
test-stream: test_stream.o 
	$(CC) -o test-stream $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

test-filter: test_filter.o
	$(CC) -o test-filter $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

test-rbtree: test_rbtree.o
	$(CC) -o test-rbtree $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

test-dlist: test_dlist.o
	$(CC) -o test-dlist $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

test-log: test_log.o
	$(CC) -o test-log $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

test-slowlog: test_slowlog.o
	$(CC) -o test-slowlog $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

test-memdev: test_memdev.o
	$(CC) -o test-memdev $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

clean:
	rm -rf *.o  $(TESTS) $(PERFS) *.gcno *.gcda *.gcov core
