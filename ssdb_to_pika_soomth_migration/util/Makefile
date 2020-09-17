CXX = g++

SRC_PATH = ./
 
CXXFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -Wall -W -DDEBUG -g -O2 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11 -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare

TARGET = libutil.a

HEADER_PATH = ./../header


SOURCE := $(wildcard $(SRC_PATH)*.cpp)

OBJS := $(patsubst %.cpp, %.o, $(SOURCE))

INCLUDE_PATH = -I./  				\
               -I$(HEADER_PATH)			\

all:$(TARGET)
	echo "creat libutil.a successful"

$(TARGET):$(OBJS)
	ar -cru $(TARGET) $(OBJS)

$(OBJS): %.o : %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

.PHONY:clean
clean:
	rm -rf $(OBJS) $(TARGET)
