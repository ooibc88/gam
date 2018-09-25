// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_STRUCTURE_H_
#define INCLUDE_STRUCTURE_H_

#include <cstdlib>
#include <cstddef>
#include <string>
#include "settings.h"
#include "log.h"
#include "locked_unordered_map.h"

typedef size_t Size;
typedef unsigned char byte;

#define DEFAULT_SPLIT_CHAR ':'

#define ALLOCATOR_ALREADY_EXIST_EXCEPTION 1
#define ALLOCATOR_NOT_EXIST_EXECEPTION 2

typedef uint64_t ptr_t;

typedef uint64_t Key;
typedef uint64_t GAddr;
#define OFF_MASK 0xFFFFFFFFFFFFL
#define WID(gaddr) ((gaddr) >> 48)
#define OFF(gaddr) ((gaddr) & OFF_MASK)
#define TO_GLOB(addr, base, wid) ((ptr_t)(addr) - (ptr_t)(base) + ((ptr_t)(wid) << 48))
#define EMPTY_GLOB(wid) ((ptr_t)(wid) << 48)

#define GADD(addr, off) ((addr)+(off)) //for now, we don't have any check for address overflow
#define GMINUS(a, b) ((a)-(b)) //need to guarantee WID(a) == WID(b)
#define TOBLOCK(x) (((ptr_t)x) & BLOCK_MASK)
#define BLOCK_ALIGNED(x) (!((x) & ~BLOCK_MASK))
#define BADD(addr, i) TOBLOCK((addr) + (i)*BLOCK_SIZE) //return an addr
#define BMINUS(i, j) (((i)-(j))>>BLOCK_POWER)
#define TO_LOCAL(gaddr, base)  (void*)(OFF(gaddr) + (ptr_t)(base))
#define Gnullptr 0

struct Conf {
  bool is_master = true;  //mark whether current process is the master (obtained from conf and the current ip)
  int master_port = 12345;
  std::string master_ip = "localhost";
  std::string master_bindaddr;
  int worker_port = 12346;
  std::string worker_bindaddr;
  std::string worker_ip = "localhost";
  Size size = 1024 * 1024L * 512;  //per-server size of memory pre-allocated
  Size ghost_th = 1024 * 1024;
  double cache_th = 0.15;  //if free mem is below this threshold, we start to allocate memory from remote nodes
  int unsynced_th = 1;
  double factor = 1.25;
  int maxclients = 1024;
  int maxthreads = 10;
  int backlog = TCP_BACKLOG;
  int loglevel = LOG_WARNING;
  std::string* logfile = nullptr;
  int timeout = 10;  //ms
  int eviction_period = 100;  //ms
};

typedef int PostProcessFunc(int, void*);

#define LOCK_MICRO(table, key) do {((table).lock(key));} while(0)
#define UNLOCK_MICRO(table, key) ((table).unlock(key))

#endif /* INCLUDE_STRUCTURE_H_ */
