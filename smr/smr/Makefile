# Makefile
CC = gcc

DEFS = -D_GNU_SOURCE -D_REENTRANT
OPT = -O0
DEBUG = -g -ggdb
INCLUDES = 

CFLAGS = -std=c99 -pedantic -Wall $(INCLUDES) $(DEFS) $(OPT) $(DEBUG) $(GCOV_FLAGS)
LINKS = -lpthread -lrt

OBJECT = tcp.o crc16.o dlist.o stream.o rbtree.o sfi.o gpbuf.o log_scan.o log_addr.o log_util.o log_disk.o log_mem.o log_recover.o log_log.o smr_be.o
MYLIB = libsmr.a

all: $(OBJECT) $(MYLIB) smr-logutil

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage -DSFI_ENABLED"

sfi:
	$(MAKE) DEBUG="-g -ggdb -DSFI_ENABLED"

# gcc -MM *.c generates below dependencies
tcp.o : tcp.c
crc16.o : crc16.c
dlist.o : dlist.c
stream.o : stream.c
rbtree.o : rbtree.c
sfi.o : sfi.c
gpbuf.o : gpbuf.c
log_scan.o : log_scan.c
log_addr.o : log_addr.c
log_util.o : log_util.c
log_disk.o : log_disk.c
log_mem.o : log_mem.c
log_recover.o : log_recover.c
log_log.o : log_log.c
smr_be.o : smr_be.c

$(MYLIB): $(OBJECT)
	ar rcs $(MYLIB) $(OBJECT)

smr-logutil : smr_logutil.c
	$(CC) -o smr-logutil $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $(LINKS) $<

clean:
	rm -rf *.o  $(MYLIB) smr-logutil *.gcno *.gcda *.gcov *~
