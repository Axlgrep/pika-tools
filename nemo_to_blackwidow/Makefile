CXX = g++
CXXFLAGS = -std=c++11 -g

TARGET = nemo_to_blackwidow

TOP_PATH = $(shell pwd)
THIRD_PATH=$(TOP_PATH)/../third
SRC_PATH=$(TOP_PATH)

ROCKSDB_PATH=$(THIRD_PATH)/rocksdb
ROCKSDB=$(ROCKSDB_PATH)/librocksdb.a

NEMO_PATH=$(THIRD_PATH)/nemo
NEMO=$(NEMO_PATH)/lib/libnemo.a

NEMO_ROCKSDB_PATH=$(THIRD_PATH)/nemo-rocksdb
NEMO_ROCKSDB=$(NEMO_ROCKSDB_PATH)/lib/libnemodb.a

SLASH_PATH=$(THIRD_PATH)/slash
SLASH=$(SLASH_PATH)/slash/lib/libslash.a

PINK_PATH=$(THIRD_PATH)/pink
PINK=$(PINK_PATH)/pink/lib/libpink.a

BLACKWIDOW_PATH=$(THIRD_PATH)/blackwidow
BLACKWIDOW=$(BLACKWIDOW_PATH)/lib/libblackwidow.a

INCLUDE_PATH = -I./                             \
               -I$(NEMO_PATH)/include           \
               -I$(NEMO_ROCKSDB_PATH)/include   \
               -I$(BLACKWIDOW_PATH)/include     \
               -I$(SLASH_PATH)                  \
               -I$(PINK_PATH)                   \
               -I$(ROCKSDB_PATH)/include        \

LIB_PATH     = -L$(NEMO_PATH)/lib               \
               -L$(NEMO_ROCKSDB_PATH)/lib       \
               -L$(BLACKWIDOW_PATH)/lib         \
               -L$(PINK_PATH)/pink/lib          \
               -L$(SLASH_PATH)/slash/lib        \
               -L$(ROCKSDB_PATH)/               \

LIBS         = -lnemo                           \
               -lnemodb                         \
               -lblackwidow                     \
               -lpink                           \
               -lslash                          \
               -lrocksdb                        \
               -lpthread                        \
               -lsnappy                         \
               -lz                              \
               -lbz2                            \
               -lglog                           \
               -lgflags                         \

SOURCE := $(wildcard $(TOP_PATH)/*.cc)
OBJS := $(patsubst %.cc, %.o, $(SOURCE))

$(TARGET): $(OBJS) $(NEMO) $(NEMO_ROCKSDB) $(BLACKWIDOW) $(PINK) $(SLASH) $(ROCKSDB)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)


$(NEMO) :
	make -C $(NEMO_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH) NEMODB_PATH=$(NEMO_ROCKSDB_PATH)

$(NEMO_ROCKSDB) :
	make -C $(NEMO_ROCKSDB_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH)

$(BLACKWIDOW) : 
	make -C $(BLACKWIDOW_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH) SLASH_PATH=$(SLASH_PATH)

$(PINK) :
	make -C $(PINK_PATH)/pink SLASH_PATH=$(SLASH_PATH)

$(SLASH) :
	make -C $(SLASH_PATH)/slash

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) -j 24 static_lib


.PHONY: clean distclean

clean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)

distclean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)
	make -C $(NEMO_PATH) clean
	make -C $(NEMO_ROCKSDB_PATH) clean
	make -C $(BLACKWIDOW_PATH) clean
	make -C $(PINK_PATH)/pink clean
	make -C $(SLASH_PATH)/slash clean
	make -C $(ROCKSDB_PATH) clean
