// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_SETTINGS_H_
#define INCLUDE_SETTINGS_H_

using namespace std;
#define UNDER_DEVELOPMENT
//#define DEBUG
//#define LOCAL_MEMORY_HOOK

//#define NDEBUG

#define USE_SIMPLE_MAP

#define HARDWARE_CACHE_LINE 64

#define ASYNC_UNLOCK

//#define FINE_SLAB_LOCK

/*
 * test switch
 */
//#define WH_USE_LOCK
// #define USE_LRU
#define LRU_NUM 10
//#define USE_APPR_LRU
//#define SELECTIVE_CACHING
#define GFUNC_SUPPORT

//#define USE_PIPE_W_TO_H
//#define USE_PIPE_H_TO_W
#define USE_BOOST_QUEUE
//#define USE_BUF_ONLY
#define MULTITHREAD
#define USE_CITYHASH
//#define USE_MURMURHASH

#define MERGE_RDMA_REQUESTS
//#define USE_PTHREAD_COND

//#define USE_HUGEPAGE

#define USE_COCKOOHASH

//#define NOCACHE
//#define ASYNC_RDMA_SEND

#define RDMA_POLL
//#define MULTITHREAD_RECV
//#define USE_BOOST_THREADPOOL

#define USE_ATOMIC

#ifdef USE_PIPE_H_TO_W
#define USE_LOCAL_TIME_EVENT
#endif

#define MAX_RW_TIME 20 //in microsecond

#define MAX_SHARED_LOCK 254 //MAX(unsigned char)-1
#define EXCLUSIVE_LOCK_TAG 255 //MAX(unsigned char)

// #define BLOCK_POWER 9
// #define BLOCK_MASK 0xFFFFFFFFFFFFFE00L

#define BLOCK_POWER 12
#define BLOCK_MASK 0xFFFFFFFFFFFFF000L

#define BLOCK_SIZE (1 << BLOCK_POWER)

#define RDMA_RESOURCE_EXCEPTION 1
#define RDMA_CONTEXT_EXCEPTION 2
#define SERVER_NOT_EXIST_EXCEPTION 3
#define SERVER_ALREADY_EXIST_EXCEPTION 4

#define MIN_RESERVED_FDS 32
#define EVENTLOOP_FDSET_INCR (MIN_RESERVED_FDS+96)
#define EVENTLOOP_FDSET_INCR (MIN_RESERVED_FDS+96)

#define TCP_BACKLOG       511     /* TCP listen backlog */
#define IP_STR_LEN 46 /* INET6_ADDRSTRLEN is 46, but we need to be sure */

#define MAX_CQ_EVENTS 1024

#define MAX_NUM_WORKER 20
#define MAX_MASTER_PENDING_MSG 1024
#define MAX_UNSIGNALED_MSG 512

#define HW_MAX_PENDING 16351
#define MAX_WORKER_PENDING_MSG 1024
#define MASTER_RDMA_SRQ_RX_DEPTH \
    (MAX_MASTER_PENDING_MSG * MAX_NUM_WORKER)
#define WORKER_RDMA_SRQ_RX_DEPTH \
    (MAX_WORKER_PENDING_MSG * (MAX_NUM_WORKER-1) + MAX_MASTER_PENDING_MSG)

#define MAX_REQUEST_SIZE 1024
#define WORKER_BUFFER_SIZE (MAX_WORKER_PENDING_MSG * MAX_REQUEST_SIZE)
#define MASTER_BUFFER_SIZE (MAX_MASTER_PENDING_MSG * MAX_REQUEST_SIZE)

#define MAX_IPPORT_STRLEN 21 //192.168.154.154:12345
#define MAX_WORKERS_STRLEN (MAX_NUM_WORKER*MAX_IPPORT_STRLEN+MAX_NUM_WORKER-1)  //192.168.154.154:12345,

#define INIT_WORKQ_SIZE 2000

#define MAX_MEM_STATS_SIZE  64 //10+10+20+20+4 in decimal

/* add ergeda add */

enum DataState {
  MSI=0,
  READ_ONLY,
  READ_MOSTLY,
  ACCESS_EXCLUSIVE,
  WRITE_EXCLUSIVE,
  WRITE_SHARED,
  RC_WRITE_SHARED
};

/* add ergeda add */


/* add wpq add */
// #define ReleaseConsistency

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)

/* add wpq add */

#endif /* INCLUDE_SETTINGS_H_ */