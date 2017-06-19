CXX      = g++
CXXFLAGS = -std=c++11 -g -O2 -Wall -I$(HDRDIR) -I. -pthread
LDFLAGS  = -lgrpc -lgrpc++ \
	   -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed \
	   -lprotobuf -lpthread -ldl
ARFLAGS  = rcs
PROTOC   = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

SRCDIR = src
GENDIR = gen
OBJDIR = build
HDRDIR = include
PBDIR = $(SRCDIR)/proto

PROTO_OBJECTS := $(OBJDIR)/crocks.pb.o \
	$(OBJDIR)/crocks.grpc.pb.o \
	$(OBJDIR)/etcd.pb.o \
	$(OBJDIR)/etcd.grpc.pb.o \
	$(OBJDIR)/etcd_lock.pb.o \
	$(OBJDIR)/etcd_lock.grpc.pb.o \
	$(OBJDIR)/info.pb.o \

COMMON_SOURCES := $(wildcard $(SRCDIR)/common/*.cc)
COMMON_OBJECTS := $(PROTO_OBJECTS) \
	$(COMMON_SOURCES:$(SRCDIR)/%.cc=$(OBJDIR)/%.o)

COMMON_SOURCES := $(wildcard $(SRCDIR)/common/*.cc)
COMMON_OBJECTS := $(PROTO_OBJECTS) \
	$(COMMON_SOURCES:$(SRCDIR)/%.cc=$(OBJDIR)/%.o)

SERVER_SOURCES := $(wildcard $(SRCDIR)/server/*.cc)
SERVER_OBJECTS := $(PROTO_OBJECTS) $(COMMON_OBJECTS) \
	$(SERVER_SOURCES:$(SRCDIR)/%.cc=$(OBJDIR)/%.o)

CLIENT_SOURCES := $(wildcard $(SRCDIR)/client/*.cc)
CLIENT_OBJECTS := $(PROTO_OBJECTS) $(COMMON_OBJECTS) \
	$(CLIENT_SOURCES:$(SRCDIR)/%.cc=$(OBJDIR)/%.o)

.PHONY: all
all: crocks crocksctl

crocks: LDFLAGS += -lrocksdb
crocks: $(SERVER_OBJECTS)
	@echo "Linking     $@"
	@$(CXX) $^ $(LDFLAGS) -o $@

crocksctl: $(PROTO_OBJECTS) $(CLIENT_OBJECTS) $(OBJDIR)/crocksctl/crocksctl.o
	@echo "Linking     $@"
	@$(CXX) $^ $(LDFLAGS) -o $@

.PHONY: test
test: test_node test_cluster test_batch test_iterator test_wrong_shard \
	test_lock test_lock2 test_migrations
test_%: $(CLIENT_OBJECTS) $(OBJDIR)/test/test_%.o
	@echo "Linking     $@"
	@$(CXX) $^ $(LDFLAGS) -o $@

# Protobuf and gRPC C++ code generation
.PRECIOUS: $(GENDIR)/%.grpc.pb.cc
$(GENDIR)/%.grpc.pb.cc: $(PBDIR)/%.proto
	@mkdir -p $(@D)
	@echo "Generating  $@"
	@$(PROTOC) -I $(PBDIR) --grpc_out=$(GENDIR) \
		--plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: $(GENDIR)/%.pb.cc
$(GENDIR)/%.pb.cc: $(PBDIR)/%.proto
	@mkdir -p $(@D)
	@echo "Generating  $@"
	@$(PROTOC) -I $(PBDIR) --cpp_out=$(GENDIR) $<

vpath %.cc $(SRCDIR)

# Generate dependency files (*.d) using -MMD and include them
# to recompile *.cc files when an included header is changed.
$(OBJDIR)/%.o: %.cc
	@mkdir -p $(@D)
	@echo "Compiling   $<"
	@$(CXX) $(CXXFLAGS) -MMD -c $< -o $@

# We need a different rule for $(GENDIR) because vpath
# cannot find sources that do not exist yet.
$(OBJDIR)/%.o: $(GENDIR)/%.cc
	@mkdir -p $(@D)
	@echo "Compiling   $<"
	@$(CXX) $(CXXFLAGS) -MMD -c $< -o $@

-include $(wildcard $(OBJDIR)/**/*.d)

# Shared library
PIC_OBJECTS := $(CLIENT_OBJECTS:%.o=%.pic.o)

.PHONY: shared
shared: libcrocks.so

libcrocks.so: LDFLAGS += -shared
libcrocks.so: CXXFLAGS += -fPIC
libcrocks.so: $(PIC_OBJECTS)
	@echo "Linking     $@"
	@$(CXX) $^ $(LDFLAGS) -o $@

$(OBJDIR)/%.pic.o: $(SRCDIR)/%.cc
	@mkdir -p $(@D)
	@echo "Compiling   $<"
	@$(CXX) $(CXXFLAGS) -MMD -c $< -o $@

$(OBJDIR)/%.pic.o: $(GENDIR)/%.cc
	@mkdir -p $(@D)
	@echo "Compiling   $<"
	@$(CXX) $(CXXFLAGS) -MMD -c $< -o $@

# Static library
.PHONY: static
static: libcrocks.a

libcrocks.a: $(CLIENT_OBJECTS)
	@echo "Linking     $@"
	@$(AR) $(ARFLAGS) $@ $^

# Installation
PREFIX ?= /usr/local

.PHONY: install
install: install-crocks install-crocksctl install-shared

.PHONY: install-crocks
install-crocks: crocks
	@echo "Installing  $<"
	@install -s -m 755 crocks $(PREFIX)/bin

.PHONY: install-crocksctl
install-crocksctl: crocksctl
	@echo "Installing  $<"
	@install -s -m 755 crocksctl $(PREFIX)/bin

.PHONY: install-headers
install-headers:
	@echo "Installing  public headers"
	@install -d $(PREFIX)/include/crocks
	@install -m 644 $(HDRDIR)/crocks/*.h $(PREFIX)/include/crocks

.PHONY: install-shared
install-shared: libcrocks.so install-headers
	@echo "Installing  $<"
	@install -d $(PREFIX)/lib
	@install -s -m 755 $< $(PREFIX)/lib

.PHONY: install-static
install-static: libcrocks.a install-headers
	@echo "Installing  $<"
	@install -d $(PREFIX)/lib
	@install -s -m 755 $< $(PREFIX)/lib

.PHONY: uninstall
uninstall:
	rm -rf $(PREFIX)/bin/crocks \
		$(PREFIX)/bin/crocksctl \
		$(PREFIX)/lib/libcrocks.so \
		$(PREFIX)/lib/libcrocks.a \
		$(PREFIX)/include/crocks

.PHONY: clean
clean:
	rm -rf gen build crocks crocksctl libcrocks.so libcrocks.a test_*
