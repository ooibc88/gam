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
#include <list>
#include "settings.h"
#include "structure.h"
#include "client.h"
#include "workrequest.h"
#include "server.h"
#include "ae.h"
#include "slabs.h"

#include "farm_txn.h"

struct TxnCommitStatus{
  std::unordered_map<uint16_t, int> progress_;
  int remaining_workers_;
  int success;
  bool local;
};

#define REQUEST_WRITE_IMM 1
#define REQUEST_SEND 1 << 1
#define REQUEST_READ 1 << 2
#define REQUEST_SIGNALED 1 << 3
#define REQUEST_NO_ID 1 << 4
#define ADD_TO_PENDING 1 << 5

class Worker: public Server {

  //the handle to the worker thread
  thread* st;

  /*
   * TODO: two more efficient strategies
   * 1) use pipe for each thread directly to transfer the pointer (then wqueue is not needed)
   * -- too many wakeups?
   * 2) use a single pipe for all the threads (thread->worker), and process all the requests once it is waked up
   * -- too much contention in the single pipe?
   * NOTE: both strategies require one pipe for each thread in (worker->thread) direction
   * in order to wake up individual thread
   */
  boost::lockfree::queue<WorkRequest*>* wqueue; //work queue used to communicate with local threads
  Client* master;
  unordered_map<int, int> pipes; //worker pipe fd to app thread pipe fd
  unsigned int wr_psn; //we assume the pending works will not exceed INT_MAX

  /*
   * the pending work requests from remote nodes
   * because some states are in intermediate state
   */
  unordered_map<GAddr, queue<pair<Client*, WorkRequest*>>> to_serve_requests;

  /*
   * the pending work requests from local nodes
   * because some states are in intermediate state
   */
  unordered_map<GAddr, queue<WorkRequest*>> to_serve_local_requests;

  void* base; //base addr
  Size size;

  Size ghost_size; //the locally allocated size that is not synced with Master

#ifndef USE_BOOST_QUEUE
  list<volatile int*> nbufs;
#endif

#ifdef DEBUG
  map<GAddr, Size> alloc;
#endif

  std::unordered_map<uint32_t, TxnContext*> local_txns_;
  std::unordered_map<uint32_t, std::unique_ptr<TxnCommitStatus>> tx_status_;

  /* contain for each client the farm requests/replies that are ready to 
   * send to this client
   */
  std::unordered_map<Client*, std::list<TxnContext*>> client_tasks_;

  /* record the TxnContext information for each remote txn; 
   * only used for comit phase
   * */
  std::unordered_map<uint64_t, std::unique_ptr<TxnContext>> remote_txns_;
  std::unordered_map<uint64_t, uint32_t> nobj_processed;

  unordered_map<uint64_t, pair<void*, Size>> kvs;

  int FarmSubmitRequest(Client* cli, WorkRequest* wr); 

  void FarmAddTask(Client*, TxnContext*);

  /* prepare local transaction */
  void FarmPrepare(TxnContext*, TxnCommitStatus*);
  void FarmPrepare(Client* c, TxnContext*);
  void FarmResumePrepare(TxnContext*, TxnCommitStatus* = nullptr);

  /* validate local transaction */
  void FarmValidate(TxnContext*, TxnCommitStatus*);
  void FarmValidate(Client* c, TxnContext*);
  void FarmResumeValidate(TxnContext*, TxnCommitStatus* = nullptr);

  /* commit/abort local transactions */
  void FarmCommitOrAbort(TxnContext*, TxnCommitStatus*);
  void FarmCommitOrAbort(Client* c, TxnContext*);

  void FarmFinalizeTxn(TxnContext*, TxnCommitStatus*);
  void FarmFinalizeTxn(Client*, TxnContext*); 

  void FarmProcessRemoteRequest(Client*, const char* msg, uint32_t len);
  void FarmProcessPrepare(Client*, TxnContext*);
  void FarmProcessValidate(Client*, TxnContext*);
  void FarmProcessCommit(Client*, TxnContext*);
  void FarmProcessAbort(Client*, TxnContext*);
  void FarmProcessPrepareReply(Client*, TxnContext*);
  void FarmProcessValidateReply(Client*, TxnContext*);
  void FarmProcessAcknowledge(Client*, TxnContext*);
  void FarmProcessMalloc(Client*, TxnContext*);
  void FarmProcessMallocReply(Client*, TxnContext*);
  void FarmProcessRead(Client*, TxnContext*);
  void FarmProcessReadReply(Client*, TxnContext*);
  void FarmWrite(std::unordered_map<GAddr, std::shared_ptr<Object>>&);

  void FarmResumeTxn(Client*);

  void FarmProcessPendingReads(TxnContext*);
  void FarmProcessPendingReads(WorkRequest*);

  /* worker serves as the coodinator for local commit requests */
  int FarmCommit(WorkRequest*);

  bool FarmAddressLocked(GAddr addr);
  bool FarmAddressRLocked(GAddr addr);
  bool FarmAddressWLocked(GAddr addr);
  bool FarmRLock(GAddr addr);
  bool FarmWLock(GAddr addr);
  void FarmUnRLock(GAddr addr);
  void FarmUnWLock(GAddr addr);

  void* FarmMalloc(osize_t, osize_t = 1);
  void FarmFree(GAddr);
  inline osize_t FarmAllocSize(char* addr) {
    return *(osize_t*)(addr - sizeof(osize_t));
  }

  void FarmAllocateTxnId(WorkRequest*);

  public:

  /* process requests PreparePreparePreparePrepareissued by local threads */
  void FarmProcessLocalRequest(WorkRequest*);
  void FarmProcessLocalMalloc(WorkRequest*);
  void FarmProcessLocalRead(WorkRequest*);
  void FarmProcessLocalCommit(WorkRequest*);

  SlabAllocator sb;
  /*
   * 1) init local address and register with the master
   * 2) get a cached copy of the whole picture about the global memory allocator
   */
  Worker(const Conf& conf, RdmaResource* res = nullptr);
  inline void Join() {st->join();}

  inline bool IsMaster() {return false;}
  inline int GetWorkerId() {return master->GetWorkerId();}

  /*
   * register the worker handle with this worker
   * return: app thread-side fd
   */
  int RegisterHandle(int fd, aeFileProc* handle = ProcessLocalRequest);
  void DeRegisterHandle(int fd);

#ifndef USE_BOOST_QUEUE
  int RegisterNotifyBuf(volatile int* notify_buf);
  void DeRegisterNotifyBuf(volatile int* notify_buf);
#endif

  inline boost::lockfree::queue<WorkRequest*>* GetWorkQ() {return wqueue;}
  inline unsigned int GetWorkPsn() {
    ++wr_psn; 
    if (wr_psn == 0) ++wr_psn;
    return wr_psn;
  }

  static void ProcessLocalRequest(aeEventLoop *el, int fd, void *data, int mask);
  void ProcessRequest(Client*, WorkRequest*) override {}

  //post process after connect to master
  int PostConnectMaster(int fd, void* data);
  void RegisterMemory(void* addr, Size s);

  /*
   * if addr == nullptr, return a random remote client
   * otherwise, return the client for the worker maintaining the addr
   */
  Client* GetClient(GAddr addr = Gnullptr);
  inline bool IsLocal(GAddr addr) {return WID(addr) == GetWorkerId();}
  inline void* ToLocal(GAddr addr) {epicAssert(IsLocal(addr)); return TO_LOCAL(addr, base);}
  inline GAddr ToGlobal(void* ptr) {return TO_GLOB(ptr, base, GetWorkerId());}

  void SyncMaster(Work op = UPDATE_MEM_STATS, WorkRequest* parent = nullptr);

  static int LocalRequestChecker(struct aeEventLoop *eventLoop, long long id, void *clientData);

  int Notify(WorkRequest* wr);

  static void StartService(Worker* w);

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
