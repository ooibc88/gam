// Copyright (c) 2018 The GAM Authors 

#include <dirent.h>
#include <pthread.h>
#include <cstdio>
#include <cerrno>
#include <atomic>
#include <sys/time.h>
#include <sys/types.h>
#include <cstdlib>
#include <cstdint>

#include "gallocator.h"
#include "util.h"
#include "kvclient.h"
#include "kv.h"

using namespace dht;

constexpr uint32_t KEY_SIZE = 100;
constexpr uint32_t REPORT = 100000;

int is_master = 0;
const char* ip_master = "10.10.10.119"; //get_local_ip("eth0").c_str();
const char* ip_worker = "localhost"; // get_local_ip("eth0").c_str();
int port_master = 9991;
int port_worker = 9992;
int no_thread = 1;
int no_client = 1;
int get_ratio = 100;
int val_size = 32;
int cache_th = 30;
int client_id = 0;
int iter = 40000;
const char *ycsb_dir = "/data/caiqc/zipf";

pthread_t *threads;
pthread_mutex_t cnt_mutex = PTHREAD_MUTEX_INITIALIZER;    
pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
int ccount = 0;

char* value;

Conf conf;
GAlloc *alloactors;

// performance counters
std::atomic<uint64_t> get_latency(0);
std::atomic<uint64_t> set_latency(0);
std::atomic<uint64_t> get_finished(0);
std::atomic<uint64_t> set_finished(0);
std::atomic<uint64_t> finished(1);

static uint64_t nstime(void) {
  struct timespec time;
  uint64_t ust;

  clock_gettime(CLOCK_MONOTONIC, &time);

  ust = ((uint64_t)time.tv_sec)*1000000000;
  ust += time.tv_nsec;
  return ust;
}

static uint64_t mstime(void) {
  return nstime()/1000000;
}

void populate(FILE * fp, kvClient* cli) {
  char key[KEY_SIZE];
  uint64_t start = mstime();
  uint64_t last_report = start, current;
  fprintf(stdout, "start to populate\n");
  int cnt = 0;
  while(cnt++ < iter && fgets(key, KEY_SIZE, fp)) {
    key[strlen(key) - 1] = 0;
    cli->put(key, value);
    if (finished.fetch_add(1, std::memory_order_relaxed) % REPORT == 0) {
      current = mstime();
      printf("%.2f\n", (1000.0 * REPORT)/(current - last_report));
      last_report = current;
    }
  }

  double duration = mstime() - start;
  printf("%lu, %.2f\n", 
      finished.load(), 
      (finished-1) * 1000/duration); 
}

void benchmark(FILE* fp, kvClient* cli) {
  uint64_t start = mstime();
  char key[KEY_SIZE];
  kv* kv;
  unsigned int seed = rand();
  uint64_t t1, t2;
  uint64_t last_report = start, current;
  int cnt = 0;
  while(cnt++ < iter && fgets(key, KEY_SIZE, fp)) {
    key[strlen(key) - 1] = 0;
    if (GetRandom(0, 100, &seed) < get_ratio) {
      t1 = nstime();
      cli->get(std::stoull(std::string(key)), &kv);
      epicAssert(!strncmp(kv->value, value, kv->vlen));
      t2 = nstime();
      get_latency += (t2 - t1);
      get_finished++;
    } else {
      t1 = nstime();
      cli->put(key, value);
      t2 = nstime();
      set_latency += (t2 - t1);
      set_finished++;
    }

    if (finished.fetch_add(1, std::memory_order_relaxed) % REPORT == 0) {
      current = mstime();
      printf("%.2f\n", (1000.0 * REPORT)/(current - last_report));
      last_report = current;
    }
  }

  double duration = mstime() - start;
  double gets = get_finished.load(), sets = set_finished.load();
  double glat = get_latency.load(), slat = set_latency.load();
  printf("%lu, %.2f", finished - 1, (finished-1) * 1000/duration); 
  if (gets > 0) printf(", %.2f", glat/gets);
  else printf(", -");
  if (sets > 0) printf(", %.2f", slat/sets);
  else printf(", -");
  printf("\n");
}

void* do_work(void* fname) {
  FILE *fp = fopen((char*)fname, "r");
  if (!fp) {
    perror("fopen:");
    exit(1);
  }

  GAlloc* alloc = GAllocFactory::CreateAllocator(&conf);
  kvClient* cli = new kvClient(alloc);

  rewind(fp);
  populate(fp, cli);

  char v = 1;
  char* vs = new char[no_client];
  int i = 0;

  // synchronize among population threads
  pthread_mutex_lock(&cnt_mutex);
  if (++ccount % no_thread == 0) {
    alloc->Put(client_id, &v, 1);
    for (i = 0; i < no_client; i++) {
      vs[i] = 0;
      while (vs[i] != 1) alloc->Get(i, &vs[i]);
    }
    pthread_cond_broadcast(&cv);
  } else {
    while(ccount % no_thread != 0) {
      pthread_cond_wait(&cv, &cnt_mutex);
    }
  }
  pthread_mutex_unlock(&cnt_mutex);

  finished.store(1);
  rewind(fp);
  benchmark(fp, cli);

  epicLog(LOG_WARNING, "warmup");
  // synchronize among benchmark threads
  pthread_mutex_lock(&cnt_mutex);
  if (++ccount % no_thread == 0) {
    v = 2;
    alloc->Put(client_id, &v, 1);
    for (i = 0; i < no_client; i++) {
      vs[i] = 0;
      while (vs[i] != 2) alloc->Get(i, &vs[i]);
    }
    pthread_cond_broadcast(&cv);
  } else {
    while(ccount % no_thread != 0) {
      pthread_cond_wait(&cv, &cnt_mutex);
    }
  }
  pthread_mutex_unlock(&cnt_mutex);

  finished.store(1);
  rewind(fp);
  benchmark(fp, cli);

  // synchronize among benchmark threads
  pthread_mutex_lock(&cnt_mutex);
  if (++ccount % no_thread == 0) {
    v = 3;
    alloc->Put(client_id, &v, 1);
    for (i = 0; i < no_client; i++) {
      vs[i] = 0;
      while (vs[i] != 3) alloc->Get(i, &vs[i]);
    }
    pthread_cond_broadcast(&cv);
  } else {
    while(ccount % no_thread != 0) {
      pthread_cond_wait(&cv, &cnt_mutex);
    }
  }
  pthread_mutex_unlock(&cnt_mutex);

}

int main(int argc, char* argv[]) {

  for(int i = 1; i < argc; i++) {
    if(strcmp(argv[i], "--ip_master") == 0) {
      ip_master = argv[++i];
    } else if(strcmp(argv[i], "--ip_worker") == 0) {
      ip_worker = argv[++i];
    } else if (strcmp(argv[i], "--port_master") == 0) {
      port_master = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--port_worker") == 0) {
      port_worker = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--is_master") == 0) {
      is_master = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--no_thread") == 0) {
      no_thread = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--client_id") == 0) {
      client_id = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--no_client") == 0) {
      no_client = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--get_ratio") == 0) {
      get_ratio = atoi(argv[++i]); //0..100
    } else if (strcmp(argv[i], "--val_size") == 0) {
      val_size = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--cache_th") == 0) {
      cache_th = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--ycsb_dir") == 0) {
      ycsb_dir = argv[++i];
    } else {
      fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
    }
  }

  conf.loglevel = LOG_WARNING;
  conf.is_master = is_master;
  conf.master_ip = ip_master;
  conf.master_port = port_master;
  conf.worker_ip = ip_worker;
  conf.worker_port = port_worker;
  conf.size = (1UL << 34); // 16GB
  conf.cache_th = (double)cache_th/100;

  GAlloc* alloc = GAllocFactory::CreateAllocator(&conf);

  if (cache_th <= 100) {
    value = new char[val_size];
    for (int i = 0; i < val_size; i++)
      value[i] = 'x';
    value[val_size - 1] = 0;
    threads = new pthread_t[no_thread];
    struct dirent * ent = NULL;
    DIR* dir = opendir(ycsb_dir);
    if (!dir) {
      perror("opendir:");
      exit(1);
    }

    int i = 0;
    for (i = 0; i < no_thread; ++i)
    {
      do {
        ent = readdir(dir);
      } while (ent && ent->d_type != DT_REG);

      if (!ent) break;

      char fname[500];
      memset(fname, 0, 500);
      strcpy(fname, ycsb_dir);
      if (fname[strlen(fname) - 1] != '/')
        fname[strlen(fname)] = '/';
      strcat(fname, ent->d_name);
      sleep(10);
      pthread_create(&threads[i], NULL, do_work, fname);
    }

    for (i--; i >= 0; i--)
      pthread_join(threads[i], NULL);

  } else {
    while (1) sleep(3600);
  }
}
