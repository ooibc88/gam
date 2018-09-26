// Copyright (c) 2018 The GAM Authors 

#ifndef INCLUDE_WORKER_H_
#define INCLUDE_WORKER_H_

#include <boost/lockfree/queue.hpp>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <queue>
#include <thread>
#include <utility>
#include <mutex>
#include <atomic>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <syscall.h>
#include "settings.h"
#include "structure.h"
#include "client.h"
#include "workrequest.h"
#include "server.h"
#include "ae.h"
#include "directory.h"
#include "cache.h"
#include "slabs.h"
#include "zmalloc.h"
#include "hashtable.h"
#include "lockwrapper.h"
#include "util.h"
#include "logging.h"

#define REQUEST_WRITE_IMM 1
#define REQUEST_SEND 1 << 1
#define REQUEST_READ 1 << 2
#define REQUEST_SIGNALED 1 << 3
#define REQUEST_NO_ID 1 << 4
#define ADD_TO_PENDING 1 << 5
#define REQUEST_ASYNC 1 << 6

class Cache;

struct Fence {
  bool sfenced = false;bool mfenced = false;
  atomic<int> pending_writes;
  queue<WorkRequest*> pending_works;bool in_process = false;
  LockWrapper lock_;
  void lock() {
    lock_.lock();
  }
  void unlock() {
    lock_.unlock();
  }
};

#ifdef ASYNC_RDMA_SEND
struct RDMASendData {
  Client* client;
  void* buf;
  size_t len;
  unsigned int id;
  bool signaled;
  RDMASendData(Client* cli, void* b, size_t l, unsigned int id = 0, bool signaled = false):
  client(cli), buf(b), len(l), id(id), signaled(signaled) {}
};
#endif

class Worker : public Server {
  friend class Cache;

  //the handle to the worker thread
  thread* st;

#ifdef USE_LRU
#ifdef USE_APPR_LRU
  long global_clock_;
#endif
#endif

  /*
   * TODO: two more efficient strategies
   * 1) use pipe for each thread directly to transfer the pointer (then wqueue is not needed)
   * -- too many wakeups?
   * 2) use a single pipe for all the threads (thread->worker), and process all the requests once it is waked up
   * -- too much contention in the single pipe?
   * NOTE: both strategies require one pipe for each thread in (worker->thread) direction
   * in order to wake up individual thread
   */
  boost::lockfree::queue<WorkRequest*>* wqueue;  //work queue used to communicate with local threads
#ifdef ASYNC_RDMA_SEND
  boost::lockfree::queue<RDMASendData*>* rdma_queue;
#endif
#ifdef USE_BOOST_THREADPOOL
  boost::asio::io_service ioService;
  boost::thread_group threadpool;
  boost::asio::io_service::work work;
  static void RdmaHandler(Worker* w, ibv_wc& wc) {
    w->ProcessRdmaRequest(wc);
  }
#endif
  Client* master;
  //unordered_map<int, int> pipes; //worker pipe fd to app thread pipe fd
  atomic<unsigned int> wr_psn;  //we assume the pending works will not exceed INT_MAX

  /*
   * pending_works: the work requests that are waiting for replies
   */
  //unordered_map<unsigned int, WorkRequest*> pending_works;
  HashTable<unsigned int, WorkRequest*> pending_works { "pending_works" };

  /*
   * the pending work requests from remote nodes
   * because some states are in intermediate state
   */
  //unordered_map<GAddr, queue<pair<Client*, WorkRequest*>>> to_serve_requests;
#ifdef USE_SIMPLE_MAP
  Map<GAddr, queue<pair<Client*, WorkRequest*>>*> to_serve_requests {
      "to_serve_requests" };
#else
  HashTable<GAddr, queue<pair<Client*, WorkRequest*>>*> to_serve_requests {"to_serve_requests"};
#endif

  /*
   * the pending work requests from local nodes
   * because some states are in intermediate state
   */
#ifdef USE_SIMPLE_MAP
  Map<GAddr, queue<WorkRequest*>*> to_serve_local_requests {
      "to_serve_local_requests" };
#else
  //unordered_map<GAddr, queue<WorkRequest*>> to_serve_local_requests;
  HashTable<GAddr, queue<WorkRequest*>*> to_serve_local_requests {"to_serve_local_requests"};
#endif

  /*
   *per thread fence data
   */
  //unordered_map<int, Fence> fences; //worker-side receive pipe -> fence structure for that thread
  HashTable<int, Fence*> fences_ { "fences_" };

  Directory directory;
  Cache cache;

  //read-only data after init
  void* base;  //base addr
  Size size;

  atomic<Size> ghost_size;  //the locally allocated size that is not synced with Master

#ifdef DHT
  void* htable = nullptr;
#endif

  Log* log;

#ifndef USE_BOOST_QUEUE
  list<volatile int*> nbufs;
#endif

 public:
  // cahce hit ratio statistics
  // number of local reads absorbed by the cache
  atomic<Size> no_local_reads_;
  atomic<Size> no_local_reads_hit_;

  // number of local writes absorbed by the cache
  atomic<Size> no_local_writes_;
  atomic<Size> no_local_writes_hit_;

  // number of remote reads absorbed by the cache
  atomic<Size> no_remote_reads_;
  atomic<Size> no_remote_reads_hit_;

  // number of remote writes absorbed by the cache
  atomic<Size> no_remote_writes_;
  atomic<Size> no_remote_writes_hit_;
  atomic<Size> no_remote_writes_direct_hit_;

  // logging
  void logWrite(GAddr addr, Size sz, const void* content) {
    //log->logWrite(addr, sz, content);
  }

  void logOwner(int id, GAddr addr) {
    //log->logOwner(id, addr);
  }
  

  SlabAllocator sb;
  /*
   * 1) init local address and register with the master
   * 2) get a cached copy of the whole picture about the global memory allocator
   */
  Worker(const Conf& conf, RdmaResource* res = nullptr);
  inline void Join() {
    st->join();
  }

  inline bool IsMaster() {
    return false;
  }
  inline int GetWorkerId() {
    return master->GetWorkerId();
  }

  /*
   * register the worker handle with this worker
   * return: app thread-side fd
   */
  int RegisterHandle(int fd, aeFileProc* handle = ProcessLocalRequest);
  void DeRegisterHandle(int fd);
  inline int RegisterFence(int fd) {
    epicAssert(fences_.count(fd) == 0);
    Fence* fence = new Fence();
    fence->pending_writes = 0;
    fences_[fd] = fence;
    return 0;
  }

#ifndef USE_BOOST_QUEUE
  int RegisterNotifyBuf(volatile int* notify_buf);
  void DeRegisterNotifyBuf(volatile int* notify_buf);
#endif

  inline boost::lockfree::queue<WorkRequest*>* GetWorkQ() {
    return wqueue;
  }
  inline unsigned int GetWorkPsn() {
    volatile unsigned int ret = ++wr_psn;
    if (ret == 0)
      ret = ++wr_psn;
    return ret;
  }

  static void ProcessLocalRequest(aeEventLoop *el, int fd, void *data,
                                  int mask);
  int ProcessLocalRequest(WorkRequest* wr);
  int ProcessLocalMalloc(WorkRequest* wr);
  int ProcessLocalFree(WorkRequest* wr);
  int ProcessLocalWrite(WorkRequest* wr);
  int ProcessLocalRead(WorkRequest* wr);
  int ProcessLocalWLock(WorkRequest* wr);
  int ProcessLocalRLock(WorkRequest* wr);
  int ProcessLocalUnLock(WorkRequest* wr);
  int ProcessLocalMFence(WorkRequest* wr);
  int ProcessLocalSFence(WorkRequest* wr);
  void ProcessRequest(Client* client, WorkRequest* wr);
  void ProcessRemoteMemStat(Client* client, WorkRequest* wr);
  void ProcessRemoteMalloc(Client* client, WorkRequest* wr);
  void ProcessRemoteMallocReply(Client* client, WorkRequest* wr);
  void ProcessRemoteGetReply(Client* client, WorkRequest* wr);
  void ProcessRemoteRead(Client* client, WorkRequest* wr);
  void ProcessRemoteReadCache(Client* client, WorkRequest* wr);
  void ProcessRemoteReadReply(Client* client, WorkRequest* wr);
  void ProcessRemoteWrite(Client* client, WorkRequest* wr);
  void ProcessRemoteWriteCache(Client* client, WorkRequest* wr);
  void ProcessRemoteWriteReply(Client* client, WorkRequest* wr);
  void ProcessRemoteEvictShared(Client* client, WorkRequest* wr);
  void ProcessRemoteEvictDirty(Client* client, WorkRequest* wr);
  void ProcessRequest(Client* client, unsigned int work_id);
  void ProcessPendingRequest(Client* cli, WorkRequest* wr);
  void ProcessPendingRead(Client* cli, WorkRequest* wr);
  void ProcessPendingReadForward(Client* cli, WorkRequest* wr);
  void ProcessPendingWrite(Client* cli, WorkRequest* wr);
  void ProcessPendingWriteForward(Client* cli, WorkRequest* wr);
  void ProcessPendingEvictDirty(Client* cli, WorkRequest* wr);
  void ProcessPendingInvalidateForward(Client* cli, WorkRequest* wr);
  void ProcessToServeRequest(WorkRequest* wr);

#ifdef DHT
  int ProcessLocalHTable(WorkRequest* wr);
	void ProcessRemoteHTable(Client* client, WorkRequest* wr);
	void ProcessHTableReply(Client* client, WorkRequest* wr);
#endif

#ifdef NOCACHE
  void ProcessRemoteWLock(Client* client, WorkRequest* wr);
  void ProcessRemoteRLock(Client* client, WorkRequest* wr);
  void ProcessRemoteUnLock(Client* client, WorkRequest* wr);
  void ProcessRemoteLockReply(Client* client, WorkRequest* wr);
  void ProcessRemoteUnLockReply(Client* client, WorkRequest* wr);
#endif

  //post process after connect to master
  int PostConnectMaster(int fd, void* data);
  void RegisterMemory(void* addr, Size s);

  /*
   * if addr == nullptr, return a random remote client
   * otherwise, return the client for the worker maintaining the addr
   */
  Client* GetClient(GAddr addr = Gnullptr);
  size_t GetWorkersSize();
  inline bool IsLocal(GAddr addr) {
    return WID(addr) == GetWorkerId();
  }
  inline void* ToLocal(GAddr addr) {
    epicAssert(IsLocal(addr));
    return TO_LOCAL(addr, base);
  }
  inline GAddr ToGlobal(void* ptr) {
    return TO_GLOB(ptr, base, GetWorkerId());
  }

  void SyncMaster(Work op = UPDATE_MEM_STATS, WorkRequest* parent = nullptr);
  unsigned long long SubmitRequest(Client* cli, WorkRequest* wr,
                                   int flag = REQUEST_SEND | REQUEST_NO_ID,
                                   void* dest = nullptr, void* src = nullptr,
                                   Size size = 0, uint32_t imm = 0);
  void AddToPending(unsigned int id, WorkRequest* wr);
  int ErasePendingWork(unsigned int id);
  WorkRequest* GetPendingWork(unsigned int id);
  int GetAndErasePendingWork(unsigned int id, WorkRequest** wp);
  inline bool IsFenced(Fence* fence, WorkRequest* wr) {
    return (fence->mfenced || fence->sfenced) && !(wr->flag & REPEATED)
        && !(wr->flag & TO_SERVE) && !(wr->flag & FENCE);
  }
  inline bool IsMFenced(Fence* fence, WorkRequest* wr) {
    return (fence->mfenced) && !(wr->flag & REPEATED) && !(wr->flag & TO_SERVE)
        && !(wr->flag & FENCE);
  }
  void ProcessFenced(Fence* fence);

  inline void AddToFence(Fence* fence, WorkRequest* wr) {
    if ((wr->flag & ASYNC)) {
      //copy the workrequest
      WorkRequest* nw = wr->Copy();
      nw->flag |= FENCE;
      //we are sure that it is not called by the thread
      //who are processing the fenced requests
      //as IsMFenced/IsFenced has checked the FENCE flag
      //fence->lock();
      fence->pending_works.push(nw);
      //fence->unlock();
    } else {
      //fence->lock();
      wr->flag |= FENCE;
      fence->pending_works.push(wr);
      //fence->unlock();
    }
  }
  inline void AddToServeLocalRequest(GAddr addr, WorkRequest* wr) {
    WorkRequest* nw = wr;
    LOCK_MICRO(to_serve_local_requests, addr);
    epicAssert(!(nw->flag & ASYNC) || nw->IsACopy());
    if (to_serve_local_requests.count(addr)) {
      auto* entry = to_serve_local_requests.at(addr);
      entry->push(nw);
    } else {
      auto* entry = new queue<WorkRequest*>();
      entry->push(nw);
      to_serve_local_requests[addr] = entry;
    }
    UNLOCK_MICRO(to_serve_local_requests, addr);
  }
  inline void AddToServeRemoteRequest(GAddr addr, Client* client,
                                      WorkRequest* wr) {
#ifdef SELECTIVE_CACHING
    addr = TOBLOCK(addr);
#endif
    WorkRequest* nw = wr;
    epicAssert(BLOCK_ALIGNED(addr));
    LOCK_MICRO(to_serve_requests, addr);
    if (to_serve_requests.count(addr)) {
      auto* entry = to_serve_requests.at(addr);
      entry->push(pair<Client*, WorkRequest*>(client, wr));
    } else {
      auto* entry = new queue<pair<Client*, WorkRequest*>>();
      entry->push(pair<Client*, WorkRequest*>(client, wr));
      to_serve_requests[addr] = entry;
    }
    UNLOCK_MICRO(to_serve_requests, addr);
  }

  void CompletionCheck(unsigned int id);

  static int LocalRequestChecker(struct aeEventLoop *eventLoop, long long id,
                                 void *clientData);

#ifdef USE_LRU
  static int CacheEvictor(struct aeEventLoop* eventLoop, long long id,
                          void* clientData) {
    Worker* w = (Worker*) clientData;
#ifdef USE_APPR_LRU
    w->SetClock(get_time());
#endif
    w->cache.Evict();
    return w->conf->eviction_period;
  }

#ifdef USE_APPR_LRU
  inline long GetClock() {
    retrun global_clock_;
  }

  inline void SetClock(long time) {
    global_clock_ = time;
  }
#endif
#endif

  int Notify(WorkRequest* wr);

  static void StartService(Worker* w);
  static void AsyncRdmaSendThread(Worker* w);

  ~Worker();
};

class WorkerFactory {
  static Worker *server;
 public:
  static Server* GetServer() {
    if (server)
      return server;
    else
      throw SERVER_NOT_EXIST_EXCEPTION;
  }
  static Worker* CreateServer(const Conf& conf) {
    if (server)
      throw SERVER_ALREADY_EXIST_EXCEPTION;
    server = new Worker(conf);
    return server;
  }
  ~WorkerFactory() {
    if (server)
      delete server;
  }
};

#endif /* INCLUDE_WORKER_H_ */
