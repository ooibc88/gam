// Copyright (c) 2018 The GAM Authors 

#include <thread>
#include <ctime>
#include <atomic>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include <mutex>
#include <cstdlib>
#include "zmalloc.h"
#include "util.h"
#include "gallocator.h"
#include <sys/time.h>

#define POPULATE 1
#define TEST 2
#define DONE 3

#define DEBUG_LEVEL LOG_INFO

int is_master = 0;
string ip_master = get_local_ip("eth0");
string ip_worker = get_local_ip("eth0");
int port_master = 12345;
int port_worker = 12346;
int obj_size = 100;
int num_obj = 1000000;
int no_thread = 10;
int no_node = 1;
int node_id = 0;
int write_ratio = 50;
int sync_key; 
int iteration = 5000;
int txn_nobj = 40;
Conf conf;

GAlloc** alloc;
char *v;

static void sync(int phase, int tid) {
  fprintf(stdout, "Thread %d leaves phase %d, start snyc now\n", tid, phase);
  uint64_t base = num_obj * no_node * no_thread;
  sync_key = base + node_id * no_thread + tid;
  alloc[tid]->Put(sync_key, &phase, sizeof(int));
  if (phase != DONE || is_master)
    for (int i = 0; i < no_thread * no_node; ++i)
    {
      //if (i == node_id * no_thread + tid) continue;
      int t;
      do {
        alloc[tid]->Get(base + i, &t);
        if (t == DONE) break;
        usleep(50000);
      } while(t != phase);
    }
}


static void farm_test(int tid) {
  int total = num_obj * no_node * no_thread;
  char t[obj_size];
  srand(node_id);

  GAddr *a = new GAddr[total];
  for (int i = 0; i < total; ++i)
  {
    if(alloc[tid]->Get(i, &a[i]) == -1)
    {
      fprintf(stdout, "tid %d, i %d\n", tid, i);
      exit(0);
    }
    int wid = a[i]>>48;
    if (wid <= 0 || wid > no_node) {
      fprintf(stdout, "tid %d, i %d, addr %lx\n", tid, i, a[i]);
      assert(wid > 0 && wid <= no_node);
    }
  }

  fprintf(stdout, "Thread %d start testing now\n", tid);
  uint64_t nr_commit = 0;
  uint64_t nr_abort = 0;
  struct timeval start, end;
  gettimeofday(&start, NULL);
  for (int j = 0; j < iteration; ++j)
  {
    //if (j % 10 == 0)
    //    fprintf(stdout, "Thread %d: iteration %d\n", tid, j);

    alloc[tid]->txBegin();
    for (int i = 0; i < txn_nobj; ++i)
    {
      int r = rand();
      //alloc[tid]->txRead(a[r%total], v, obj_size);
      if (r % 100 < write_ratio)
        alloc[tid]->txWrite(a[r%total], v, obj_size);
      else {
        alloc[tid]->txRead(a[r%total], t, obj_size);
        //assert(strncmp(v, t, obj_size) == 0);
        //memset(t, 0, obj_size);
      }
    }

    if (alloc[tid]->txCommit() == SUCCESS)
      nr_commit++;
    else nr_abort++;
  }

  gettimeofday(&end, NULL);
  double time = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
  fprintf(stdout, "time:%f s, nr_commit = %ld, nr_abort = %ld, ratio = %f, thruput = %f\n", 
      (time)/1000000,
      nr_commit,
      nr_abort,
      (float)nr_commit/(nr_commit + nr_abort),
      (nr_abort + nr_commit)*1000000/time);

  alloc[tid]->txCommit();

  delete a;
  sync(TEST, tid);
  sync(DONE, tid);
}


static void populate(int tid) {
  v = new char[obj_size];
  memset(v, 0, obj_size);

  fprintf(stdout, "thread %d start populate\n", tid);
  GAddr* a = new GAddr[num_obj];
  alloc[tid]->txBegin();
  for (int i = 0; i < num_obj; ++i)
  {
    a[i] = alloc[tid]->txAlloc(obj_size);
    int wid = a[i]>>48;
    if (wid <= 0 || wid > no_node)
      assert(wid > 0 && wid <= no_node);
    alloc[tid]->txWrite(a[i], v, obj_size);
  }

  if (!alloc[tid]->txCommit()) {
    for (int i = 0; i < num_obj; ++i)
    {
      if(-1 == alloc[tid]->Put(node_id * num_obj * no_thread + tid * num_obj + i, &a[i], sizeof(GAddr)))
        exit(0);
    }
  } else {
    exit(0);
  }
  sync(POPULATE, tid);
  delete a;
}

int main(int argc, char* argv[]) {
  //the first argument should be the program name
  for(int i = 1; i < argc; i++) {
    if(strcmp(argv[i], "--ip_master") == 0) {
      ip_master = string(argv[++i]);
    } else if(strcmp(argv[i], "--ip_worker") == 0) {
      ip_worker = string(argv[++i]);
    } else if (strcmp(argv[i], "--port_master") == 0) {
      port_master = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--iface_master") == 0) {
      ip_master = get_local_ip(argv[++i]);
    } else if (strcmp(argv[i], "--port_worker") == 0) {
      port_worker = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--iface_worker") == 0) {
      ip_worker = get_local_ip(argv[++i]);
    } else if (strcmp(argv[i], "--iface") == 0) {
      ip_worker = get_local_ip(argv[++i]);
      ip_master = get_local_ip(argv[i]);
    } else if (strcmp(argv[i], "--is_master") == 0) {
      is_master = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--obj_size") == 0) {
      obj_size = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--num_obj") == 0) {
      num_obj = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--no_node") == 0) {
      no_node = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--node_id") == 0) {
      node_id = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--write_ratio") == 0) {
      write_ratio = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--iteration") == 0) {
      iteration = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--txn_nobj") == 0) {
      txn_nobj = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--no_thread") == 0) {
      no_thread = atoi(argv[++i]);
    } else {
      fprintf(stdout, "Unrecognized option %s for benchmark\n", argv[i]);
    }
  }

  conf.loglevel = DEBUG_LEVEL;
  conf.is_master = is_master;
  conf.master_ip = ip_master;
  conf.master_port = port_master;
  conf.worker_ip = ip_worker;
  conf.worker_port = port_worker;
  conf.size = 1024 * 1024 * 1024;

  alloc = new GAlloc*[no_thread];
  for (int i = 0; i < no_thread; ++i)
  {
    alloc[i] = GAllocFactory::CreateAllocator(&conf);
  }

  sleep(2);

  thread* th[no_thread];
  for (int i = 0; i < no_thread; ++i)
  {
    th[i] = new thread(populate, i);
  }

  for (int i = 0; i < no_thread; ++i)
  {
    th[i]->join();
  }

  for (int i = 0; i < no_thread; ++i)
  {
    th[i] = new thread(farm_test, i);
  }

  for (int i = 0; i < no_thread; ++i)
  {
    th[i]->join();
  }
}
