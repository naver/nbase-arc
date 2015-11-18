INSTALL_DIR32=.lib32
INSTALL_DIR64=.lib64

ifeq ($(CFLAGS),-m32)
    INSTALL_DIR=$(INSTALL_DIR32)
else
    INSTALL_DIR=$(INSTALL_DIR64)
endif

all:
	-cd c && $(MAKE) clean
	cd c && ./configure --prefix=$(PWD)/$(INSTALL_DIR)
	cd c && $(MAKE) CFLAGS=$(CFLAGS)
	cd c && $(MAKE) install

clean:
	-cd c && $(MAKE) $@
	rm -rf $(INSTALL_DIR64) $(INSTALL_DIR32)
