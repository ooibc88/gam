// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_GALLOCATOR_H_
#define INCLUDE_GALLOCATOR_H_

#include <cstring>
#include "lockwrapper.h"
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#ifdef GFUNC_SUPPORT
#include "gfunc.h"
#endif

class GAlloc {
  WorkerHandle* wh;  //handle to communicate with local worker

  int Lock(Work op, const GAddr addr, const Size count, Flag flag = 0);
 public:
  GAlloc(Worker* worker);

  /**
   * malloc in the global address
   * @param size: the size of memory to be malloced
   * @param flag: some special flags
   * @param base: used to guarantee the affinity,
   * 				trying to put the newly-allocated addr to be in the same node of the base addr
   */
  GAddr Malloc(const Size size, Flag flag = 0);
  GAddr Malloc(const Size size, GAddr base, Flag flag = 0);
  GAddr AlignedMalloc(const Size size, GAddr base, Flag flag = 0);
  GAddr AlignedMalloc(const Size size, Flag flag = 0);
  GAddr Calloc(Size nmemb, Size size, Flag flag, GAddr base);
  GAddr Realloc(GAddr ptr, Size size, Flag flag);

  void Free(const GAddr addr);

  /*
   * read is blocking until buf is filled with requested data
   */
  int Read(const GAddr addr, void* buf, const Size count, Flag flag = 0);
  int Read(const GAddr addr, const Size offset, void* buf, const Size count,
           Flag flag = 0);

  /*
   * Generally, write is non-blocking as we're using release memory consistency model
   */
  int Write(const GAddr addr, void* buf, const Size count, Flag flag = 0);
#ifdef GFUNC_SUPPORT
  int Write(const GAddr addr, const Size offset, void* buf, const Size count,
            Flag flag = 0, GFunc* func = nullptr, uint64_t arg = 0);
  int Write(const GAddr addr, void* buf, const Size count, GFunc* func,
            uint64_t arg = 0, Flag flag = 0);
#else
  int Write(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag = 0);
#endif

  void MFence();
  void SFence();

  void RLock(const GAddr addr, const Size count);
  void WLock(const GAddr addr, const Size count);

  void UnLock(const GAddr addr, const Size count);

  int Try_RLock(const GAddr addr, const Size count);
  int Try_WLock(const GAddr addr, const Size count);

  Size Put(uint64_t key, const void* value, Size count);
  Size Get(uint64_t key, void* value);

#ifdef DHT
  int HTable(void*);
#endif

  inline int GetID() {
    return wh->GetWorkerId();
  }
  int GetWorkersSize() {
    return wh->GetWorkersSize();
  }

  /*
   * check whether addr is a local addr or not
   */
  inline bool IsLocal(GAddr addr) {
    return WID(addr) == GetID();
  }

  /*
   * return the local pointer of the global address addr
   * if not local, return nullptr
   */
  inline void* GetLocal(GAddr addr) {
    if (!IsLocal(addr)) {
      return nullptr;
    } else {
      return wh->GetLocal(addr);
    }
  }

  void ReportCacheStatistics() {
    wh->ReportCacheStatistics();
  }

  void ResetCacheStatistics() {
    wh->ResetCacheStatistics();
  }

  ~GAlloc();
};

class GAllocFactory {
  static const Conf* conf;
  static Worker* worker;
  static Master* master;
  static LockWrapper lock;
 public:
#ifdef GFUNC_SUPPORT
#define MAX_GFUNCS 100
  static GFunc* gfuncs[MAX_GFUNCS];
#endif
  /*
   * this function should be call in every thread
   * in order to init some per-thread data
   */
  static GAlloc* CreateAllocator(const std::string& conf_file) {
    return CreateAllocator(ParseConf(conf_file));
  }

  static const Conf* InitConf() {
    lock.lock();
    conf = new Conf();
    const Conf* ret = conf;
    lock.unlock();
    return ret;
  }

  //need to call for every thread
  static GAlloc* CreateAllocator(const Conf* c = nullptr) {
    lock.lock();
    if (c) {
      if (!conf) {
        conf = c;
      } else {
        epicLog(LOG_INFO, "NOTICE: Conf already exist %lx", conf);
      }
    } else {
      if (!conf) {
        epicLog(LOG_FATAL, "Must provide conf for the first time");
      }
    }

    if (conf->is_master) {
      if (!master)
        master = MasterFactory::CreateServer(*conf);
    }
    if (!worker) {
      worker = WorkerFactory::CreateServer(*conf);
    }
    GAlloc* ret = new GAlloc(worker);
    lock.unlock();
    return ret;
  }

  //need to call for every thread
  static GAlloc* CreateAllocator(const Conf& c) {
    lock.lock();
    if (!conf) {
      Conf* lc = new Conf();
      *lc = c;
      conf = lc;
    } else {
      epicLog(LOG_INFO, "Conf already exist %lx", conf);
    }
    if (conf->is_master) {
      if (!master)
        master = MasterFactory::CreateServer(*conf);
    }
    if (!worker) {
      worker = WorkerFactory::CreateServer(*conf);
    }
    GAlloc* ret = new GAlloc(worker);
    lock.unlock();
    return ret;
  }

  static void SetConf(Conf* c) {
    lock.lock();
    conf = c;
    lock.unlock();
  }

  static void FreeResouce() {
    lock.lock();
    delete conf;
    delete worker;
    delete master;
    lock.unlock();
  }

  /*
   * TODO: fake parseconf function
   */
  static const Conf* ParseConf(const std::string& conf_file) {
    Conf* c = new Conf();
    return c;
  }

  static int LogLevel() {
    int ret = conf->loglevel;
    return ret;
  }

  static string* LogFile() {
    string* ret = conf->logfile;
    return ret;
  }
};

#endif /* INCLUDE_GALLOCATOR_H_ */
