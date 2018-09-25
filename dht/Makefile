CPP = g++ 
INCLUDE = -I../include -I../lib/libcuckoo/src -I../lib/libcuckoo/cityhash-1.1.1/src 
LIBS = ../src/libgalloc.a -libverbs -lpthread ../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a -lboost_thread -lboost_system
#LIBS = ../src/libgalloc.a -libverbs -lpthread ../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a /users/zhongle/local/lib/libboost_thread.so /users/zhongle/local/lib/libboost_system.so
CFLAGS += -rdynamic -std=c++11 -O3 -DDHT

%.o: %.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -c -o $@ $^ 

benchmark: kvbench.o kvclient.o
	cd ../src; make clean; make dht=1 -j; cd ../dht
	$(CPP) -o $@ $^ $(LIBS) 

clean:
	rm *.o benchmark

