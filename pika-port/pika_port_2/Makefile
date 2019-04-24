CXX = g++

ifeq ($(__REL), 1)
  CXXFLAGS = -O2 -g -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith \
             -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter \
             -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
  CXXFLAGS = -O0 -g -pg -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch \
             -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE \
	           -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls
endif

TARGET = pika_port
SRC_DIR = .
THIRD_PATH = ../third

# ROCKSDB_LDFLAGS= -lsnappy -lz -lbz2 -llz4 -lzstd
ROCKSDB_LDFLAGS= -lsnappy  -lgflags -lunwind -lz -lbz2 -llz4
TCMALLOC_EXTENSION_FLAGS=
TCMALLOC_LDFLAGS=
PROCESSOR_NUMS=2

NEMO_PATH=$(THIRD_PATH)/nemo
NEMO=$(NEMO_PATH)/lib/libnemo.a

NEMO_ROCKSDB_PATH=$(THIRD_PATH)/nemo-rocksdb
NEMO_ROCKSDB=$(NEMO_ROCKSDB_PATH)/lib/libnemodb.a

UNWIND_PATH=../deps/libunwind
GFLAGS_PATH=../deps/gflags
GLOG_PATH=../deps/glog
NEMO=$(NEMO_PATH)/lib/libnemo.a
PIKA_PATH=./pika2.3.6

INCLUDE_PATH = -I$(GLOG_PATH)/include \
	   -I$(PIKA_PATH)/ \
	   -I$(PIKA_PATH)/include \
	   -I$(THIRD_PATH)/slash \
	   -I$(THIRD_PATH)/pink \
	   -I$(THIRD_PATH)/nemo/include/ \
	   -I$(THIRD_PATH)/nemo-rocksdb/include/ \
	   -I$(THIRD_PATH)/rocksdb/include/

LIB_PATH = -L./ \
	   -L$(GLOG_PATH)/lib/ \
	   -L$(GFLAGS_PATH)/lib/ \
	   -L$(UNWIND_PATH)/lib/ \
	   -L$(THIRD_PATH)/nemo-rocksdb/lib \
	   -L$(THIRD_PATH)/rocksdb/ \
	   -L$(THIRD_PATH)/slash/slash/lib \
	   -L$(THIRD_PATH)/pink/pink/lib \
     -L$(THIRD_PATH)/nemo/lib/

LIBS = -Wl,-Bstatic -lnemo -lrocksdb\
       -Wl,-Bdynamic -lpthread\
       -lrt \
       -lglog \
       -lpink \
       -lslash \
       -lnemodb \
       -lrocksdb

LIBS += $(ROCKSDB_LDFLAGS)

GLOG = $(GLOG_PATH)/lib/libglog.a
PINK = $(THIRD_PATH)/pink/pink/lib/libpink.a
SLASH = $(THIRD_PATH)/slash/slash/lib/libslash.a

.PHONY: all clean

BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

all: $(TARGET)
	@echo "Success, go, go, go..."

# $(TARGET): $(GLOG) $(PINK) $(SLASH) $(OBJS)
$(TARGET): $(OBJS) $(NEMO) $(NEMO_ROCKSDB) $(GFLAGS) $(GLOG) $(PINK) $(SLASH)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

$(NEMO) :
	make -C $(NEMO_PATH) ROCKSDB_PATH=../rocksdb NEMODB_PATH=../nemo-rocksdb

$(NEMO_ROCKSDB) :
	make -C $(NEMO_ROCKSDB_PATH) ROCKSDB_PATH=../rocksdb

$(PINK) :
	make -C $(PINK_PATH)/pink SLASH_PATH=../../slash

$(SLASH) :
	make -C $(SLASH_PATH)/slash

clean:
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(TARGET)
	make -C $(NEMO_PATH) clean
	make -C $(NEMO_ROCKSDB_PATH) clean
	make -C $(BLACKWIDOW_PATH) clean
	make -C $(PINK_PATH)/pink clean
	make -C $(SLASH_PATH)/slash clean
	make -C $(ROCKSDB_PATH) clean
