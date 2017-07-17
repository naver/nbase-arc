# Makefile
CC = gcc

DEFS = -D_GNU_SOURCE -D_REENTRANT
OPT = -O0
DEBUG = -g -ggdb
INCLUDES = -I../smr

CFLAGS = -std=c99 -pedantic -Wall $(INCLUDES) $(DEFS) $(OPT) $(DEBUG) $(GCOV_FLAGS)
LINKS = ../smr/libsmr.a -lpthread -lrt

OBJECT = ae.o smrmp.o part_filter.o bio.o util.o applog.o slowlog.o log_mmap.o
SMR_OBJECTS = smr_replicator.o $(OBJECT)

all: $(OBJECT) smr-replicator

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage -DSFI_ENABLED"

sfi:
	$(MAKE) DEBUG="-g -ggdb -DSFI_ENABLED"

# gcc -MM *.c generates below dependencies
smr_replicator.o : smr_replicator.c
smrmp.o: smrmp.c
part_filter.o: part_filter.c
bio.o: bio.c
util.o: util.c
applog.o: applog.c
slowlog.o: slowlog.c
log_mmap.o: log_mmap.c

# executables
smr-replicator: $(SMR_OBJECTS)
	 $(CC) -o smr-replicator $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS) 

clean:
	rm -f *.o  smr-replicator *.gcno *.gcda *.gcov
