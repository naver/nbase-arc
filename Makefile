# Top level makefile
GIT_REFRESH:=$(shell git update-index --refresh)
# Note --match value is glob(7) pattern
VERSION:=$(shell git describe --tags --dirty --always --match=v[0-9]*\.[0-9]*\.[0-9]*)
#REDIS_VERSION=2.8.8
REDIS_VERSION=3.2.9
RELEASE_DIR=release/nbase-arc

default: all

.DEFAULT:
	cd smr && $(MAKE) $@
	cd redis-$(REDIS_VERSION) && $(MAKE) $@
	cd gateway && $(MAKE) $@
	cd api && $(MAKE) $@
	cd confmaster && $(MAKE) $@
	cd integration_test && $(MAKE) $@
	cd tools && $(MAKE) $@

release: all
	mkdir -p $(RELEASE_DIR)/bin
	mkdir -p $(RELEASE_DIR)/confmaster
	mkdir -p $(RELEASE_DIR)/mgmt/config
	mkdir -p $(RELEASE_DIR)/api/c
	mkdir -p $(RELEASE_DIR)/api/java
	# Redis
	cp -rf redis-$(REDIS_VERSION)/src/redis-arc $(RELEASE_DIR)/bin/redis-arc-$(VERSION)
	cp -rf redis-$(REDIS_VERSION)/src/cluster-util $(RELEASE_DIR)/bin/cluster-util-$(VERSION)
	# Confmaster
	cp -rf confmaster/target/confmaster-1.0.0-SNAPSHOT-jar-with-dependencies.jar $(RELEASE_DIR)/confmaster/confmaster-$(VERSION).jar
	cp -rf confmaster/target/cc.properties $(RELEASE_DIR)/confmaster
	sed "1,1s/confmaster-1.0.0-SNAPSHOT-jar-with-dependencies.jar/confmaster-$(VERSION).jar/g" confmaster/script/confmaster.sh > $(RELEASE_DIR)/confmaster/confmaster-$(VERSION).sh
	chmod +x $(RELEASE_DIR)/confmaster/confmaster-$(VERSION).sh
	# Gateway
	cp -rf gateway/redis-gateway $(RELEASE_DIR)/bin/redis-gateway-$(VERSION)
	# SMR
	cp -rf smr/smr/smr-logutil $(RELEASE_DIR)/bin/smr-logutil-$(VERSION)
	cp -rf smr/replicator/smr-replicator $(RELEASE_DIR)/bin/smr-replicator-$(VERSION)
	# Tools
	cp -rf tools/mgmt/*.py $(RELEASE_DIR)/mgmt/
	cp -rf tools/mgmt/config/*.py $(RELEASE_DIR)/mgmt/config
	cp -rf tools/mgmt/bash.nbase-arc $(RELEASE_DIR)/mgmt/
	cp -rf tools/local_proxy/release/local_proxy $(RELEASE_DIR)/bin/local_proxy-$(VERSION)
	cp -rf tools/nbase-arc-cli/arc-cli $(RELEASE_DIR)/bin/arc-cli-$(VERSION)
	cp -rf tools/nbase-arc-cli/arc-cli-admin $(RELEASE_DIR)/bin/arc-cli-admin-$(VERSION)
	# C API
	cp -rf api/arcci/release/arcci.h $(RELEASE_DIR)/api/c
	cp -rf api/arcci/release/libarcci.* $(RELEASE_DIR)/api/c
	# JAVA API
	cp -rf api/java/target/nbase-arc-java-client-*-sources.jar $(RELEASE_DIR)/api/java
	cp -rf api/java/target/nbase-arc-java-client-*.jar $(RELEASE_DIR)/api/java

.PHONY: release

release32:
	cd api/arcci && $(MAKE) 32bit
	cd tools/local_proxy && $(MAKE) 32bit
	mkdir -p $(RELEASE_DIR)/bin
	mkdir -p $(RELEASE_DIR)/api/c32
	# C API
	cp -rf api/arcci/release32/arcci.h $(RELEASE_DIR)/api/c32
	cp -rf api/arcci/release32/libarcci.* $(RELEASE_DIR)/api/c32
	# Tools
	cp -rf tools/local_proxy/release32/local_proxy $(RELEASE_DIR)/bin/local_proxy32-$(VERSION)

distclean: clean
	cd redis-$(REDIS_VERSION) && $(MAKE) $@
	rm -rf $(RELEASE_DIR)
	rm -rf release
