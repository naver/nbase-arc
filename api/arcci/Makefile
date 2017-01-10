# Makefile
CC=gcc
ZK_VERSION=3.4.6

# Default Settings
STD=-std=c99 -pedantic
WARN=-Wall
OPT=-O0
DEBUG=-g -ggdb

# NAMES
OBJDIR64=.obj64
OBJDIR32=.obj32
ZK_LIB_DIR64=zk/.lib64
ZK_LIB_DIR32=zk/.lib32
RELEASE_DIR64=release
RELEASE_DIR32=release32
SHARED_LIB=libarcci.so
STATIC_LIB=libarcci.a
DUMMY_PERF=dummy-perf
TEST_FI=test-fiall
ZKLIB_TMPDIR=.zk_lib_tmp
ifeq ($(CFLAGS),-m32)
    OBJDIR=$(OBJDIR32)
    ZK_LIB_DIR=$(PWD)/$(ZK_LIB_DIR32)
    RELEASE_DIR=$(PWD)/$(RELEASE_DIR32)
else
    OBJDIR=$(OBJDIR64)
    ZK_LIB_DIR=$(PWD)/$(ZK_LIB_DIR64)
    RELEASE_DIR=$(PWD)/$(RELEASE_DIR64)
endif
ZK_LIB=$(ZK_LIB_DIR)/lib/libzookeeper_st.a
$(shell mkdir -p $(OBJDIR)/lib)
$(shell mkdir -p $(OBJDIR)/include)

# CFLAGS
FINAL_CFLAGS=-fPIC $(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) $(GCOV_FLAGS)
FINAL_CFLAGS+=-I$(ZK_LIB_DIR)/include/zookeeper
FINAL_CFLAGS+=-D_REENTRANT

# LDFLAGS
FINAL_LDFLAGS=$(DEBUG) $(LDFLAGS) $(GCOV_FLAGS)

# LIBS
FINAL_LIBS=$(ZK_LIB)
FINAL_LIBS_LDFLAGS=-lpthread -lm

# OBJECT
LIB_OBJ=dict.o sds.o anet.o ae.o crc16.o arcci_common.o arcci_fe.o arcci_be.o os.o
DUMMYPERF_OBJ=$(LIB_OBJ) dummy_perf.o
TESTFI_OBJ=$(LIB_OBJ) test_fiall.o

# OBJECT WITH PREFIX
LIB_OBJ_WITH_DIR=$(addprefix $(OBJDIR)/,$(LIB_OBJ))
DUMMYPERF_OBJ_WITH_DIR=$(addprefix $(OBJDIR)/,$(DUMMYPERF_OBJ))
TESTFI_OBJ_WITH_DIR=$(addprefix $(OBJDIR)/,$(TESTFI_OBJ))

ifeq ($(shell uname -s), Darwin)
    ZK_LN=ln -sf
    SHARED_LIB_VERSION_SCRIPT=
else
    ZK_LN=ln -sfT
    SHARED_LIB_VERSION_SCRIPT=-Wl,--version-script=arcci.map
endif

all: $(STATIC_LIB) $(SHARED_LIB) $(DUMMY_PERF) $(TEST_FI)
	mkdir -p $(RELEASE_DIR)
	cp $(OBJDIR)/include/arcci.h $(RELEASE_DIR)
	cp $(OBJDIR)/lib/$(STATIC_LIB) $(RELEASE_DIR)
	cp $(OBJDIR)/lib/$(SHARED_LIB) $(RELEASE_DIR)

%.o : %.c
	$(CC) $(FINAL_CFLAGS) -c -o $(OBJDIR)/$@ $<

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32"

gcov:
	$(MAKE) GCOV_FLAGS="-fprofile-arcs -ftest-coverage -DFI_ENABLED"

gcov32:
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32" GCOV_FLAGS="-fprofile-arcs -ftest-coverage -DFI_ENABLED"

$(ZK_LIB):
	cd zk-$(ZK_VERSION) && $(MAKE) CFLAGS=$(CFLAGS)
	$(ZK_LN) zk-$(ZK_VERSION) zk

$(STATIC_LIB): $(FINAL_LIBS) $(LIB_OBJ)
	mkdir -p $(ZKLIB_TMPDIR)
	cd $(ZKLIB_TMPDIR) && ar -x $(ZK_LIB)
	ar rcs $(OBJDIR)/lib/$@ $(LIB_OBJ_WITH_DIR) $(ZKLIB_TMPDIR)/*.o
	cp arcci.h $(OBJDIR)/include
	rm -rf $(ZKLIB_TMPDIR)

$(SHARED_LIB): $(FINAL_LIBS) $(LIB_OBJ)
	$(CC) -shared $(SHARED_LIB_VERSION_SCRIPT) -o $(OBJDIR)/lib/$@ $(LIB_OBJ_WITH_DIR) $(FINAL_LDFLAGS) $(FINAL_LIBS) $(FINAL_LIBS_LDFLAGS)

$(DUMMY_PERF): $(FINAL_LIBS) $(DUMMYPERF_OBJ)
	$(CC) -o $(OBJDIR)/$@ $(DUMMYPERF_OBJ_WITH_DIR) $(FINAL_LDFLAGS) $(FINAL_LIBS) $(FINAL_LIBS_LDFLAGS)

$(TEST_FI): $(FINAL_LIBS) $(TESTFI_OBJ)
	$(CC) -o $(OBJDIR)/$@ $(TESTFI_OBJ_WITH_DIR) $(FINAL_LDFLAGS) $(FINAL_LIBS) $(FINAL_LIBS_LDFLAGS)

clean:
	cd zk-$(ZK_VERSION) && $(MAKE) $@
	rm -f zk *.o *.so *.gcno *.gcda
	rm -rf $(ZKLIB_TMPDIR) $(OBJDIR64) $(OBJDIR32) $(DUMMY_PERF) $(TEST_FI)
	rm -rf $(RELEASE_DIR64) $(RELEASE_DIR32)
