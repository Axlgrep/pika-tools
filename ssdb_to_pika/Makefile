CXX = g++
CXXFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -Wall -W -DDEBUG -g -O2 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11

OBJECT = ssdb_to_pika

SRC_DIR = .
OUTPUT = .

SSDB_CLIENT=./libssdb-client.a

THIRD=./../third

ROCKSDB_PATH=$(THIRD)/rocksdb
ROCKSDB=$(THIRD)/rocksdb/librocksdb.a
 
NEMO_PATH=$(THIRD)/nemo
NEMO=$(THIRD)/nemo/lib/libnemo.a

NEMO_ROCKSDB_PATH=$(THIRD)/nemo-rocksdb
NEMO_ROCKSDB=$(THIRD)/nemo-rocksdb/lib/libnemodb.a

BLACKWIDOW_PATH=$(THIRD)/blackwidow
BLACKWIDOW=$(THIRD)/blackwidow/lib/libblackwidow.a

SLASH_PATH=$(THIRD)/slash
SLASH=$(THIRD)/slash/slash/lib/libslash.a

INCLUDE_PATH = -I./ \
							 -I$(THIRD)/nemo/include \
							 -I$(THIRD)/nemo-rocksdb/include \
							 -I$(THIRD)/rocksdb \
							 -I$(THIRD)/rocksdb/include \
							 -I$(THIRD)/blackwidow/include/blackwidow \
							 -I$(THIRD)/blackwidow \
							 -I$(THIRD)/slash



LIBS = -lrt -pthread -lsnappy -lbz2 -lz
LIBS += $(ROCKSDB_LDFLAGS)

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(OBJS) $(NEMO) $(NEMO_ROCKSDB) $(ROCKSDB) $(BLACKWIDOW) $(SLASH) $(SSDB_CLIENT)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(NEMO) $(NEMO_ROCKSDB) $(ROCKSDB) $(BLACKWIDOW) $(SLASH)  $(LIBS) $(SSDB_CLIENT)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

$(NEMO):
	make -C $(NEMO_PATH) ROCKSDB_PATH=./../rocksdb NEMODB_PATH=./../nemo-rocksdb

$(NEMO_ROCKSDB):
	make -C $(NEMO_ROCKSDB_PATH) ROCKSDB_PATH=./../rocksdb

$(BLACKWIDOW):
	make -C $(BLACKWIDOW_PATH) ROCKSDB_PATH=./../rocksdb SLASH_PATH=./../slash

$(SLASH):
	make -C $(SLASH_PATH)/slash

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) -j 24 static_lib

clean:
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OBJECT)
