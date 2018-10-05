CPP = g++ -std=c++11
GAM_INCLUDE = -I../../include -I../ -I../../lib/libcuckoo/src -I../../lib/libcuckoo/cityhash-1.1.1/src
DB_INCLUDE = -I.. -I../bench -I../storage -I../txn -I../utils
INCLUDE = $(GAM_INCLUDE) $(DB_INCLUDE)
LIBS = ../../src/libgalloc.a -libverbs -lboost_filesystem -lboost_system -lboost_date_time -lboost_thread ../../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a
CFLAGS += -O3 -DLOCK -DBACKOFF #-DPROFILE

all: tpcc
build: tpcc

SRC = TpccBenchmarkMain.cpp TpccRandomGenerator.cpp ../Meta.cpp ../txn/TransactionManager2PL.cpp ../txn/TransactionManagerST.cpp ../utils/Profiler.cpp
OBJ = TpccBenchmarkMain.o TpccRandomGenerator.o ../Meta.o ../txn/TransactionManager2PL.o ../txn/TransactionManagerST.o ../utils/Profiler.o

$(OBJ): %.o : %.cpp
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@

tpcc: $(OBJ)
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS) 

clean:
	rm -rf tpcc *.o ../*.o ../bench/*.o ../storage/*.o ../txn/*.o ../utils/*.o
