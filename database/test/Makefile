CPP = g++ -std=c++11
GAM_INCLUDE = -I../../include -I../ -I../../lib/libcuckoo/src -I../../lib/libcuckoo/cityhash-1.1.1/src
DB_INCLUDE = -I.. -I../tpcc -I../bench -I../storage -I../txn -I../utils
INCLUDE = $(GAM_INCLUDE) $(DB_INCLUDE)
LIBS = ../../src/libgalloc.a -libverbs -lboost_filesystem -lboost_system -lboost_date_time -lboost_thread ../../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a
CFLAGS += -O3 -g -DLOCK -DBACKOFF #-DPROFILE

all: tpcc_populate_test hash_index_test
build: tpcc_populate_test hash_index_test

SM_OBJ = ../tpcc/TpccRandomGenerator.o ../Meta.o ../txn/TransactionManager2PL.o ../txn/TransactionManagerST.o ../utils/Profiler.o
OBJ = $(SM_OBJ) TpccPopulateTest.o HashIndexTest.o 
TPCC_POPULATE_OBJ = TpccPopulateTest.o $(SM_OBJ) 
HASH_INDEX_OBJ = HashIndexTest.o $(SM_OBJ) 

$(OBJ): %.o : %.cpp
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@

tpcc_populate_test: $(TPCC_POPULATE_OBJ) 
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $(TPCC_POPULATE_OBJ) $(LIBS) 

hash_index_test: $(HASH_INDEX_OBJ) 
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $(HASH_INDEX_OBJ) $(LIBS) 

clean:
	rm -rf tpcc_populate_test hash_index_test *.o ../*.o ../bench/*.o ../storage/*.o ../txn/*.o ../utils/*.o
