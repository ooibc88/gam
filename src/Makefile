CPP = g++ -std=c++11 -O3
INCLUDE = -I../include
LIBS = ./libgalloc.a -libverbs -lpthread
CFLAGS += -rdynamic #-DFARM_ENABLED

all: libgalloc.a 
test: libgalloc.a lock_test example example-r worker master rw_test fence_test benchmark
build: libgalloc.a libgalloc.so 

SRC = ae.cc client.cc server.cc worker.cc gallocator.cc master.cc tcp.cc worker_handle.cc anet.cc rdma.cc util.cc zmalloc.cc log.cc slabs.cc workrequest.cc  farm.cc farm_txn.cc
OBJ = ae.o client.o server.o worker.o gallocator.o master.o tcp.o worker_handle.o anet.o rdma.o util.o zmalloc.o log.o slabs.o workrequest.o  farm.o farm_txn.o

libgalloc.so: $(SRC)
	$(CPP) $(CFLAGS) $(INCLUDE) -fPIC -shared -o $@ $^ $(LIBS) 

$(OBJ): %.o : %.cc
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@ 

libgalloc.a: $(OBJ)
	ar rcs $@ $^ 

farm_test: ../test/farm_rw_benchmark.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)		

benchmark: ../test/benchmark.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)		

lock_test: ../test/lock_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)		
	
example: ../test/example.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS) 

example-r: ../test/example-r.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS) 
	
worker: ../test/worker_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)
	
master: ../test/master_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)
	
rw_test: ../test/rw_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)
	
fence_test: ../test/fence_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)		
	
clean_test:
	rm -rf benchmark example worker master example-r rw_test fence_test lock_test
	
clean:
	rm -rf benchmark example worker master example-r rw_test fence_test lock_test *.o *.a *.so *.d


