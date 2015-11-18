# Makefile

CC = gcc

DEFS = -D_GNU_SOURCE -D_REENTRANT
OPT = -O0
DEBUG = -g -ggdb
INCLUDES = -I../../replicator -I../../smr

CFLAGS = -std=c99 -pedantic -Wall $(INCLUDES) $(DEFS) $(OPT) $(DEBUG) $(GCOV_FLAGS)
LINKS = ../../smr/libsmr.a -lpthread -lrt

OBJECT = ../../replicator/ae.o  ../../replicator/smrmp.o
SERVER_OBJECT = smr_server.o $(OBJECT)
CLIENT_OBJECT = smr_client.o $(OBJECT)

all: smr-client smr-server

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage -DSFI_ENABLED"

# gcc -MM *.c generates below dependencies
smr_server.o: smr_server.c 
smr_client.o: smr_client.c 

# executables
smr-client: $(CLIENT_OBJECT)
	$(CC) -o smr-client $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

smr-server: $(SERVER_OBJECT)
	$(CC) -o smr-server $(DEFS) $(CFLAGS) $(DEBUG) $(OBJECT) $< $(LINKS)

clean:
	rm -rf *.o smr-client smr-server *.gcno *.gcda
