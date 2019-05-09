CXX = g++

LDFLAGS=-lpthread -lsnappy

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
	-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

CXXFLAGS = $(WARNING_FLAGS) -std=c++11 -g

THIRD_PATH=$(CURDIR)/../third

SLASH_PATH=$(THIRD_PATH)/slash
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a
PINK_PATH=$(THIRD_PATH)/pink
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a


INCLUDE_PATH = -I./                        \
               -I$(SLASH_PATH)             \
			   -I$(PINK_PATH)           \

OBJECT : SLASH PINK auto_dbsync

PINK : $(SLASH)
	make -C $(PINK_PATH)/pink NO_PB=0 SLASH_PATH=$(SLASH_PATH)

SLASH :
	make -C $(SLASH_PATH)/slash

pika_binlog.o : pika_binlog.cc
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -c $^ -o $@  $(INCLUDE_PATH) $(SLASH_LIBRARY)

auto_dbsync : auto_dbsync.cc pika_binlog.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ -o $@  $(INCLUDE_PATH) $(SLASH_LIBRARY)

clean:
	rm -rf auto_dbsync
	rm -rf pika_binlog.o

distclean:
	make -C $(SLASH_PATH)/slash clean
	make -C $(PINK_PATH)/pink clean
	rm -rf auto_dbsync
	rm -rf pika_binlog.o
