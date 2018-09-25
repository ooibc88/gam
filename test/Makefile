CPP = g++ -std=c++11 -O3
INCLUDE = -I../include
LIBS = ../src/libgalloc.a -libverbs -lpthread
CFLAGS += -g -rdynamic

farm: farm_rw_test farm_rw_benchmark farm_partial_rw_test farm_cluster_test 

farm_rw_test: farm_rw_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

farm_partial_rw_test: farm_partial_rw_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

farm_rw_benchmark: farm_rw_benchmark.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

farm_cluster_test: farm_cluster_test.cc
	$(CPP) $(CFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

clean:
	rm -rf farm_rw_test farm_rw_benchmark farm_partial_rw_test farm_cluster_test farm_exp
