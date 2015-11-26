# Makefile
CC = gcc

# Default Settings
STD=-std=c99 -pedantic
WARN=-Wall
OPT=-O0
DEBUG=-g -ggdb

# NAMES
CAPI_DIR=../../api/arcci
OBJDIR64=.obj64
OBJDIR32=.obj32
RELEASE_DIR64=release
RELEASE_DIR32=release32
LOCAL_PROXY=local_proxy
ifeq ($(CFLAGS),-m32)
    OBJDIR=$(OBJDIR32)
    RELEASE_DIR=$(RELEASE_DIR32)
else
    OBJDIR=$(OBJDIR64)
    RELEASE_DIR=$(RELEASE_DIR64)
endif
CAPI_LIB=$(CAPI_DIR)/$(OBJDIR)/lib/libarcci.a
$(shell mkdir -p $(OBJDIR))

# CFLAGS
FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) $(GCOV_FLAGS)
FINAL_CFLAGS+=-I$(CAPI_DIR)
FINAL_CFLAGS+=-D_REENTRANT -D_BSD_SOURCE -D_XOPEN_SOURCE

# LDFLAGS
FINAL_LDFLAGS=$(DEBUG) $(LDFLAGS) $(GCOV_FLAGS)

# LIBS
FINAL_LIBS=$(CAPI_LIB) -lpthread -lm

# OBJECT
OBJECT=local_proxy.o

# OBJECT WITH PREFIX
OBJ_WITH_DIR=$(addprefix $(OBJDIR)/,$(OBJECT))

all: $(LOCAL_PROXY)

%.o : %.c
	$(CC) $(FINAL_CFLAGS) -c -o $(OBJDIR)/$@ $<

32bit:
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32"

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage"

gcov32:
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32" GCOV_FLAGS="-fprofile-arcs -ftest-coverage"

$(CAPI_LIB):
	cd $(CAPI_DIR) && $(MAKE) CFLAGS=$(CFLAGS)

$(LOCAL_PROXY): $(OBJECT) $(FINAL_LIBS)
	$(CC) -o $(OBJDIR)/$@ $(OBJ_WITH_DIR) $(FINAL_LDFLAGS) $(FINAL_LIBS)
	mkdir -p $(RELEASE_DIR)
	cp $(OBJDIR)/$@ $(RELEASE_DIR)

clean:
	rm -f *.o *.gcno *.gcda
	rm -rf $(OBJDIR64) $(OBJDIR32)
	rm -rf $(RELEASE_DIR64) $(RELEASE_DIR32)
