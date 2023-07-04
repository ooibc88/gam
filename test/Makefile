CPP = g++ -std=c++11 -fopenmp -O3
INCLUDE = -I../include -I../lib/libcuckoo/src -I../lib/libcuckoo/cityhash-1.1.1/src
LIBS = ../src/libgalloc.a -libverbs -lpthread ../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a -lboost_thread -lboost_system
CFLAGS += -g -rdynamic

build: benchmark lru_test example example-r worker master slab_test rw_test fence_test lock_test hashtable_test hashtable_throw_test garray_test cs_test test
t3:fft_not_Recu.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

t2:fft_openmp.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

t1:test_fftmsi.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

t:parr_write.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

benchmark: benchmark.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS) 

lru_test: lru_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

lock_test: lock_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

fence_test: fence_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

rw_test: rw_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

slab_test: slab_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

example: example.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

example-r: example-r.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

worker: worker_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)
	
master: master_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

hashtable_test: hashtable_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

hashtable_throw_test: hashtable_throw_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)
	
garray_test: garray_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)
	
cs_test: cs_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

test: test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

gfunc_test: ../test/gfunc_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS)		

clean:
	rm -rf example example-r worker master slab_test rw_test lock_test lru_test fence_test benchmark hashtable_test hashtable_throw_test garray_test cs_test gfunc_test test
