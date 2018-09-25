// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_CACHE_H_
#define INCLUDE_CACHE_H_

#include <unordered_map>
#include <set>
#include <atomic>
#include "settings.h"
#include "structure.h"
#include "workrequest.h"
#include "hashtable.h"
#include "lockwrapper.h"
#include "map.h"
#include "util.h"

class Worker;

//we assume the base address is also BLOCK-aligned
#define GTOBLOCK(x) TOBLOCK(x)

#define CACHE_LINE_PREFIX 0 //dirty/invalid
typedef void* caddr;

enum CacheState {
  CACHE_NOT_EXIST = -1,
  CACHE_INVALID = 0,
  CACHE_SHARED,
  CACHE_DIRTY,
  CACHE_TO_INVALID,
  CACHE_TO_SHARED,
  CACHE_TO_DIRTY,
#ifdef SELECTIVE_CACHING
CACHE_NOT_CACHE
#endif
};

struct CacheLine {
  void* line = nullptr;
  GAddr addr = 0;
  CacheState state = CACHE_INVALID;
  unordered_map<GAddr, int> locks;
  //used for LRU
#ifdef USE_APPR_LRU
  long lruclock;
#else
  struct CacheLine* next = nullptr;
  struct CacheLine* prev = nullptr;
  int pos = -1;
#endif
#ifdef SELECTIVE_CACHING
  int nread = 0;
  int nwrite = 0;
  int ntoshared = 0;
  int ntoinvalid = 0;
#endif
};

class Cache {
/*
 * BLOCK-ALIGNED GAddr to local pointer
 */
//unordered_map<GAddr, CacheLine> caches;
#ifdef USE_SIMPLE_MAP
  Map<GAddr, CacheLine*> caches { "cache" };
#else
  HashTable<GAddr, CacheLine*> caches {"cache"};
#endif
  Worker* worker;

  //following are performance statistics
  atomic<long> read_miss;
  atomic<long> write_miss;
  atomic<size_t> used_bytes;
  size_t max_cache_mem = 0;

#ifdef USE_LRU
  //LRU list
  CacheLine* heads[LRU_NUM];
  CacheLine* tails[LRU_NUM];
  LockWrapper lru_locks_[LRU_NUM];
  int sample_num = LRU_NUM;
#endif

#ifdef USE_LRU
  int Evict(int n);
  void Evict(CacheLine* cline);
#endif

#ifdef SELECTIVE_CACHING

  atomic<int> nread;
  atomic<int> nwrite;
  atomic<int> ntoshared;
  atomic<int> ntoinvalid;

  inline bool IsCachable(CacheLine* cline, WorkRequest* wr) {
    epicAssert(wr->op == READ || wr->op == WRITE);
    double lambda = 0.5;
    double th = 0.2;

    int totalWR = 0;
    int curTotalWR = 0;
    int clineWR = 0;
    int curClineWR = 0;

    int totalInvalid = 0;
    int curTotalInvalid = 0; 
    int clineInvalid = 0;
    int curClineInvalid = 0;

    double idc = 0;
    //double thld = 1.236; 
    double thld = 0.5;
    int lim = 100;
    int limTotalWR = 200;
    int limClineWR = 100;
    int limTotalInvalid = 300;
    int limClineInvalid = 150;
    //int cnum = 0;
    //worker->GetClusterSize();

    if(wr->flag & NOT_CACHE) {
      return false;
    } else {
      //totalWR = nread + nwrite == 0 ? 1 : nread + nwrite;
      totalWR = nread + nwrite;
      totalWR = totalWR == 0 ? 1 : totalWR;
      totalInvalid = ntoinvalid; 
      totalInvalid = totalInvalid == 0 ? 1 : totalInvalid;
      clineWR = cline->nwrite + cline->nread;
      curTotalWR = totalWR % limTotalWR;
      curClineWR = clineWR % limClineWR;

      clineInvalid = cline->ntoinvalid;
      curTotalInvalid = totalInvalid % limTotalInvalid;
      curClineInvalid = clineInvalid % limClineInvalid;

      if(curTotalWR <= lim && totalWR <= lim) {
        return true;
      } else {
        idc = double(curClineWR)/curTotalWR + 1 - double(curClineInvalid)/curTotalInvalid;
        if(idc >= thld) {
          return true;
        } else {
          return false;
        }
      } 

    }
  }

  void InitCacheCLine(CacheLine* cline, bool write=false);
  void InitCacheCLineIfNeeded(CacheLine* cline);
#endif

  public:
  atomic<long> to_evicted;
  Cache(Worker* w);
  Cache() {} ;
  void SetWorker(Worker* w);
  void* GetLine(GAddr addr);
  inline CacheLine* GetCLine(GAddr addr) {
    GAddr block = GTOBLOCK(addr);
    if (caches.count(block)) {
      CacheLine* cline = caches.at(block);
#ifndef SELECTIVE_CACHING
      epicAssert(GetState(cline) != CACHE_INVALID);
#endif
      return cline;
    }
    return nullptr;
  }

  void* SetLine(GAddr addr, void* line = nullptr);
  CacheLine* SetCLine(GAddr addr, void* line = nullptr);
  CacheState GetState(GAddr addr);
  inline CacheState GetState(CacheLine* cline) {
    return cline->state;
  }
  void ToInvalid(GAddr addr);
  void ToInvalid(CacheLine* cline);
  void ToShared(GAddr addr);
  inline void ToShared(CacheLine* cline) {
    epicAssert(cline->state == CACHE_TO_SHARED || cline->state == CACHE_DIRTY);
#ifdef SELECTIVE_CACHING
    if(cline->state == CACHE_DIRTY) {  //dirty to shared (someone wants to read the data)
      cline->ntoshared++;
      ntoshared++;
    }
#endif
#ifdef USE_LRU
    if (cline->state == CACHE_TO_SHARED) {
      LinkLRU(cline);
    }
#endif
    cline->state = CACHE_SHARED;
  }

  void ToDirty(GAddr addr);
  inline void ToDirty(CacheLine* cline) {
    epicAssert(cline->state == CACHE_TO_DIRTY);
    cline->state = CACHE_DIRTY;
#ifdef USE_LRU
    LinkLRU(cline);
#endif
  }

  void ToToInvalid(GAddr addr);
  inline void ToToInvalid(CacheLine* cline) {
    epicAssert(cline->state == CACHE_SHARED || cline->state == CACHE_DIRTY);
    cline->state = CACHE_TO_INVALID;
    to_evicted++;
  }

  void ToToShared(GAddr addr);
  inline void ToToShared(CacheLine* cline) {
#ifndef SELECTIVE_CACHING
    epicAssert(cline->state == CACHE_INVALID);
#endif
    cline->state = CACHE_TO_SHARED;
  }

  void ToToDirty(GAddr addr);
  inline void ToToDirty(CacheLine* cline) {
#ifndef SELECTIVE_CACHING
    epicAssert(cline->state == CACHE_INVALID || cline->state == CACHE_SHARED);
#endif
    cline->state = CACHE_TO_DIRTY;
  }

  inline bool IsDirty(GAddr addr) {
    return GetState(addr) == CACHE_DIRTY;
  }

  inline bool IsDirty(CacheLine* cline) {
    return cline->state == CACHE_DIRTY;
  }

  bool InTransitionState(GAddr addr);
  inline bool InTransitionState(CacheLine* cline) {
    return InTransitionState(cline->state);
  }

  inline bool InTransitionState(CacheState s) {
    return CACHE_TO_DIRTY == s || CACHE_TO_INVALID == s || CACHE_TO_SHARED == s;
  }

  int RLock(GAddr addr);
  inline int RLock(CacheLine* cline, GAddr addr) {
    if (IsWLocked(cline, addr))
      return -1;
    if (cline->locks.count(addr)) {
      cline->locks[addr]++;
    } else {
      cline->locks[addr] = 1;
    }
    epicAssert(cline->locks[addr] <= MAX_SHARED_LOCK);
    return 0;
  }
  int RLock(CacheLine* cline) = delete;

  int WLock(GAddr addr);
  inline int WLock(CacheLine* cline, GAddr addr) {
    if (IsWLocked(cline, addr) || IsRLocked(cline, addr))
      return -1;
    cline->locks[addr] = EXCLUSIVE_LOCK_TAG;
    return 0;
  }
  int WLock(CacheLine* cline) = delete;

  bool IsWLocked(GAddr addr);
  inline bool IsWLocked(CacheLine* cline, GAddr addr) {
    return cline->locks.count(addr) && cline->locks.at(addr) == EXCLUSIVE_LOCK_TAG;
  }

  bool IsRLocked(GAddr addr);
  inline bool IsRLocked(CacheLine* cline, GAddr addr) {
    if (cline->locks.count(addr)
        == 0|| cline->locks.at(addr) == EXCLUSIVE_LOCK_TAG) {
      return false;
    } else {
      epicAssert(cline->state != CACHE_INVALID);
      epicAssert(cline->locks.at(addr) > 0);
      return true;
    }
  }

  bool IsBlockLocked(GAddr block);
  inline bool IsBlockLocked(CacheLine* cline) {
    return cline->locks.size() > 0 ? true : false;
  }

  bool IsBlockWLocked(GAddr block);
  inline bool IsBlockWLocked(CacheLine* cline) {
    bool wlocked = false;
    for (auto& entry : cline->locks) {
      if (entry.second == EXCLUSIVE_LOCK_TAG) {
        wlocked = true;
        break;
      }
    }
    return wlocked;
  }

  void UnLock(GAddr addr);
  void UndoShared(GAddr addr);
  inline void UndoShared(CacheLine* cline) {
    cline->state = CACHE_SHARED;
  }
  inline size_t GetUsedBytes() {
    return used_bytes;
  }

#ifdef USE_LRU
  void LinkLRU(CacheLine* cline);
  void UnLinkLRU(CacheLine* cline);
  void UnLinkLRU(CacheLine* cline, int pos);
  void Evict();
#endif
  /*
   * 1. check the cache
   * 2. if in the cache, copy and done
   *    if not, submit the request to the corresponding worker
   * 3. put it in the pending list (waiting for reply)
   * NOTE: a read may be divided into multiple sub-reads,
   * so the data consistency must be ensured by upper-layer app
   */
  int Lock(WorkRequest* wr);
  int RLock(WorkRequest* wr);
  int WLock(WorkRequest* wr);
  int Read(WorkRequest* wr);
  int Write(WorkRequest* wr);
  int ReadWrite(WorkRequest* wr);

  ~Cache() { };  //it is only used when program exits

  //below are used for multithread programming
  inline void lock(GAddr addr) {
    epicAssert(BLOCK_ALIGNED(addr));
    LOCK_MICRO(caches, addr);
  }
  inline bool try_lock(GAddr addr) {
    epicAssert(BLOCK_ALIGNED(addr));
    return caches.try_lock(addr);
  }
  inline void unlock(GAddr addr) {
    epicAssert(BLOCK_ALIGNED(addr));
    UNLOCK_MICRO(caches, addr);
  }
  void lock(CacheLine* cline) = delete;
  void unlock(CacheLine* cline) = delete;

#ifdef SELECTIVE_CACHING
  void ToNotCache(CacheLine* cline, bool write = false);
#endif
};

#endif /* INCLUDE_CACHE_H_ */
