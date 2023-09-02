// Copyright (c) 2018 The GAM Authors

#ifndef INCLUDE_WORKER_HANDLE_H_
#define INCLUDE_WORKER_HANDLE_H_

#include <mutex>
#include <atomic>

#include "lockwrapper.h"
#include "worker.h"
#include "workrequest.h"

class WorkerHandle
{
  boost::lockfree::queue<WorkRequest *> *wqueue; // work queue used to communicate with worker
  Worker *worker;
  // app-side pipe fd
  int send_pipe[2];
  int recv_pipe[2];
  static LockWrapper lock;
#ifdef USE_PTHREAD_COND
  pthread_mutex_t cond_lock;
  pthread_cond_t cond;
#endif
#if !(defined(USE_PIPE_W_TO_H) && defined(USE_PIPE_H_TO_W))
  volatile int *notify_buf;
  int notify_buf_size;
#endif
public:
  /* add xmx add */
  uint64_t getTransferredBytes() const {
    return worker->getTransferredBytes();
  }
  uint64_t getracetime() const {
    return worker->getracetime();
  }
  uint64_t getrequesttime() const {
    return worker->getrequesttime();
  }
#ifdef B_I
    uint64_t getreadmiss() const {
      return worker->getreadmiss();
    }
    uint64_t getreadhit() const{
      return worker->getreadhit();
    }
    uint64_t getwritemiss() const {
      return worker->getwritemiss();
    }
    uint64_t getwritehit() const {
      return worker->getwritehit();
    }
#endif
  /* add xmx add */
  WorkerHandle(Worker *w);
  void RegisterThread();
  void DeRegisterThread();
  int SendRequest(WorkRequest *wr);
  inline int GetWorkerId()
  {
    return worker->GetWorkerId();
  }
  int GetWorkersSize()
  {
    return worker->GetWorkersSize();
  }
  inline void *GetLocal(GAddr addr)
  {
    return worker->ToLocal(addr);
  }
  inline void *GetCacheLocal(GAddr addr)
  {
    return worker->GetCacheLocal(addr);
  }
  inline int FlushToHome(int workId, void *dest, void *src, int size, int id)
  {
    return worker->FlushToHome(workId, dest, src, size, id);
  }

  inline void releaseLock(int id, GAddr addr)
  {
    worker->releaseLock(id, addr);
  }

  inline void readAll(GAddr addr, int size)
  {

    WorkRequest wr{};
    wr.op = InitAcquire;
    wr.wid = GetWorkerId();
    wr.flag = 0;
    wr.size = size;
    wr.addr = addr;
    if (SendRequest(&wr))
    {
      epicLog(LOG_WARNING, "send request failed");
    }
  }

  inline void acquireLock(int id, GAddr addr, int size, bool flag, int flush_size)
  {
    worker->flush_size[id] = flush_size;
    // worker->flush_size[id] = 0;

    if (GetWorkerId() == WID(addr))
    {
      epicLog(LOG_DEBUG, "this is master, no need to acquire lock");
      return;
    }
    if (flag == true)
    {
      readAll(addr, size);
    }
    epicAssert(GetWorkerId() != WID(addr));

    epicLog(LOG_DEBUG, "read all done,workerid=%d,isAcquired=%d", GetWorkerId(), worker->is_acquired[id].load());

    worker->acquireLock(id, addr, size);
  }

  void ReportCacheStatistics();
  void ResetCacheStatistics();

  ~WorkerHandle();
};

#endif /* INCLUDE_WORKER_HANDLE_H_ */
