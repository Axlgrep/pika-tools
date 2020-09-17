CXX = g++ 

SRC_PATH=.

CXXFLAGS =  -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -Wall -W -DDEBUG -g -O2 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11 -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare



TARGET = ssdb_to_pika

THIRD = ./../third

HIREDIS_PATH = $(THIRD)/hiredis-vip
HIREDIS = $(HIREDIS_PATH)/libhiredis_vip.a

NET_PATH = ./net
NET = $(NET_PATH)/libnet.a

UTIL_PATH = ./util
UTIL = $(UTIL_PATH)/libutil.a

SSDB_PATH = ./ssdb
SSDB = $(SSDB_PATH)/libssdb.a

LEVELDB_PATH = ./leveldb-1.20
LEVELDB = $(LEVELDB_PATH)/out-static/libleveldb.a

LIB_PATH = -L$(HIREDIS_PATH) 		\
           -L$(NET_PATH)		\
           -L$(UTIL_PATH)		\
           -L$(SSDB_PATH)		\
           -L$(LEVELDB_PATH)/out-static	\

LIBS = -lssdb				\
       -lnet	     			\
       -lutil	     			\
       -lpthread     			\
       -lhiredis_vip			\
       -lleveldb			\
       -lsnappy				\
 
INCLUDE_PATH = -I./  			\
               -I./header       	\
               -I$(NET_PATH)    	\
               -I$(UTIL_PATH)   	\
               -I$(HIREDIS_PATH)	\
	       -I$(LEVELDB_PATH)/include \
	       -I$(SSDB_PATH)		\

SOURCE := $(wildcard $(SRC_PATH)/*.cc)

OBJS := $(patsubst %.cc, %.o, $(SOURCE))

all:$(TARGET)
	@rm -rf ./DB/*
	echo "succussful"

$(TARGET) : $(OBJS) $(HIREDIS) $(NET) $(UTIL) $(SSDB) $(LEVELDB)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

$(HIREDIS):
	make -C $(HIREDIS_PATH) 
	rm -f $(HIREDIS_PATH)/libhiredis_vip.so

$(UTIL):
	make -C $(UTIL_PATH)

$(NET):
	make -C $(NET_PATH)

$(LEVELDB):
	make -C $(LEVELDB_PATH)

$(SSDB):
	make -C $(SSDB_PATH)

.PHONY:clean distclean

clean:
	rm -rf ssdb_to_pika $(OBJS) ./DB/data ./DB/meta

distclean:clean
	make -C $(SSDB_PATH) clean
	make -C $(UTIL_PATH) clean
	make -C $(NET_PATH) clean
	make -C $(LEVELDB_PATH) clean
	make -C $(HIREDIS_PATH) clean
