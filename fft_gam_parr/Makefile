CPP = g++ -std=c++11 -fopenmp -O3
INCLUDE = -I../include -I../lib/libcuckoo/src -I../lib/libcuckoo/cityhash-1.1.1/src
LIBS = ../src/libgalloc.a -libverbs -lpthread ../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a -lboost_thread -lboost_system
CFLAGS += -g -rdynamic

build: test


test: test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

fft: fft.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)


clean: 
	rm -rf test fft
