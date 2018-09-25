// Copyright (c) 2018 The GAM Authors 


#include <stdio.h>
#include "gallocator.h"
#include "workrequest.h"
#include "zmalloc.h"
#include <cstring>
#include "../include/lockwrapper.h"

const Conf* GAllocFactory::conf = nullptr;
Worker* GAllocFactory::worker;
Master* GAllocFactory::master;
LockWrapper GAllocFactory::lock;
#ifdef GFUNC_SUPPORT
GFunc* GAllocFactory::gfuncs[] = { Incr, IncrDouble, GatherPagerank,
    ApplyPagerank, ScatterPagerank };
#endif

GAlloc::GAlloc(Worker* worker)
    : wh(new WorkerHandle(worker)) {
}

GAddr GAlloc::Malloc(const Size size, Flag flag) {
  return Malloc(size, Gnullptr, flag);
}
GAddr GAlloc::Malloc(const Size size, GAddr base, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  void* laddr = zmalloc(size);
  return (GAddr)laddr;
#else
  WorkRequest wr = { };
  wr.op = MALLOC;
  wr.flag = flag;
  wr.size = size;

  if (base) {
    wr.addr = base;
  }

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "malloc failed");
    return Gnullptr;
  } else {
    epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
    return wr.addr;
  }
#endif
}

GAddr GAlloc::AlignedMalloc(const Size size, Flag flag) {
  return AlignedMalloc(size, Gnullptr, flag);
}
GAddr GAlloc::AlignedMalloc(const Size size, GAddr base, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  void* ret;
  int rret = posix_memalign(&ret, BLOCK_SIZE, size);
  epicAssert(!rret && (GAddr)ret % BLOCK_SIZE == 0);
  return (GAddr)ret;
#else
  WorkRequest wr = { };
  wr.op = MALLOC;
  wr.flag = flag;
  wr.flag |= ALIGNED;
  wr.size = size;

  if (base) {
    wr.addr = base;
  }

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "malloc failed");
    return Gnullptr;
  } else {
    epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
    epicAssert(wr.addr % BLOCK_SIZE == 0);
    return wr.addr;
  }
#endif
}

GAddr GAlloc::Calloc(Size nmemb, Size size, Flag flag, GAddr base) {
  epicLog(LOG_WARNING, "not supported for now");
  return Gnullptr;
}

GAddr GAlloc::Realloc(GAddr ptr, Size size, Flag flag) {
  epicLog(LOG_WARNING, "not supported for now");
  return Gnullptr;
}

void GAlloc::Free(const GAddr addr) {
  WorkRequest wr = { };
  wr.op = FREE;
  wr.addr = addr;

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "free failed");
  } else {
    epicLog(LOG_DEBUG, "free %x:%lx succeeded!", WID(wr.addr), OFF(wr.addr));
  }
}

int GAlloc::Read(const GAddr addr, void* buf, const Size count, Flag flag) {
  return Read(addr, 0, buf, count, flag);
}
int GAlloc::Read(const GAddr addr, const Size offset, void* buf,
                 const Size count, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  char* laddr = (char*)addr;
  memcpy(buf, laddr+offset, count);
  return count;
#else
  WorkRequest wr { };
  wr.op = READ;
  wr.flag = flag;
  wr.size = count;
  wr.addr = GADD(addr, offset);
  wr.ptr = buf;

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "read failed");
    return 0;
  } else {
    return wr.size;
  }
#endif
}

#ifdef GFUNC_SUPPORT
int GAlloc::Write(const GAddr addr, void* buf, const Size count, GFunc* func,
                  uint64_t arg, Flag flag) {
  return Write(addr, 0, buf, count, flag, func, arg);
}
#endif

int GAlloc::Write(const GAddr addr, void* buf, const Size count, Flag flag) {
  return Write(addr, 0, buf, count, flag);
}

#ifdef GFUNC_SUPPORT
int GAlloc::Write(const GAddr addr, const Size offset, void* buf,
                  const Size count, Flag flag, GFunc* func, uint64_t arg) {
#else
  int GAlloc::Write(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag) {
#endif
#ifdef LOCAL_MEMORY_HOOK
  char* laddr = (char*)addr;
  memcpy(laddr+offset, buf, count);
  return count;
#else
  //for asynchronous request, we must ensure the WorkRequest is valid after this function returns
  WorkRequest wr { };
  wr.op = WRITE;
  wr.flag = flag | ASYNC;
  wr.size = count;
  wr.addr = GADD(addr, offset);
  wr.ptr = buf;
#ifdef GFUNC_SUPPORT
  if (func) {
    wr.gfunc = func;
    wr.arg = arg;
    wr.flag |= GFUNC;
  }
#endif

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "write failed");
    return 0;
  } else {
    return wr.size;
  }
#endif
}

void GAlloc::MFence() {
#ifndef LOCAL_MEMORY_HOOK
  WorkRequest wr { };
  wr.op = MFENCE;
  wr.flag = ASYNC;
  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "MFence failed");
  }
#endif
}

void GAlloc::SFence() {
#ifndef LOCAL_MEMORY_HOOK
  WorkRequest wr { };
  wr.op = SFENCE;
  wr.flag = ASYNC;
  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "SFence failed");
  }
#endif
}

int GAlloc::Lock(Work op, const GAddr addr, const Size count, Flag flag) {
#ifdef LOCAL_MEMORY_HOOK
  return 0;
#else
  WorkRequest wr { };
  wr.op = op;
  wr.addr = addr;
#ifdef ASYNC_UNLOCK
  if (op == UNLOCK)
    flag |= ASYNC;
#endif
  wr.flag = flag;
  int i = 0, j = 0;
  GAddr start_blk = TOBLOCK(addr);
  GAddr end = GADD(addr, count - 1);
  GAddr end_blk = TOBLOCK(end);
  while (!wh->SendRequest(&wr)) {
    i++;
    GAddr next = GADD(start_blk, i*BLOCK_SIZE);
    if (next > end_blk)
      break;

    epicLog(LOG_DEBUG, "lock split to multiple blocks");
    wr.Reset();
    wr.op = op;
    wr.addr = next;
    wr.flag = flag;
    epicAssert(wr.addr % BLOCK_SIZE == 0);
  }
  if (op == UNLOCK) {
    epicAssert(!wr.status);
  } else {
    if (wr.status) {  //failed at ith lock
      if (i >= 1) {  //first lock succeed
        wr.Reset();
        wr.op = UNLOCK;
        wr.addr = addr;
        wr.flag = flag;
#ifdef ASYNC_UNLOCK
        wr.flag |= ASYNC;
#endif
        int ret = wh->SendRequest(&wr);
        epicAssert(!ret);
      }
      for (j = 1; j < i; j++) {
        wr.Reset();
        wr.addr = GADD(start_blk, j*BLOCK_SIZE);
        epicAssert(wr.addr % BLOCK_SIZE == 0);
        epicAssert(wr.addr <= end_blk);
        wr.op = UNLOCK;
        wr.flag = flag;
#ifdef ASYNC_UNLOCK
        wr.flag |= ASYNC;
#endif
        int ret = wh->SendRequest(&wr);
        epicAssert(!ret);
      }
      epicLog(LOG_DEBUG, "lock failed");
      return -1;
    }
  }
  epicLog(LOG_DEBUG, "lock succeed");
  return 0;
#endif
}

void GAlloc::RLock(const GAddr addr, const Size count) {
  Lock(RLOCK, addr, count);
}

void GAlloc::WLock(const GAddr addr, const Size count) {
  Lock(WLOCK, addr, count);
}

int GAlloc::Try_RLock(const GAddr addr, const Size count) {
  return Lock(RLOCK, addr, count, TRY_LOCK);
}

int GAlloc::Try_WLock(const GAddr addr, const Size count) {
  return Lock(WLOCK, addr, count, TRY_LOCK);
}

void GAlloc::UnLock(const GAddr addr, const Size count) {
  Lock(UNLOCK, addr, count);
}

Size GAlloc::Put(uint64_t key, const void* value, Size count) {
  WorkRequest wr { };
  wr.op = PUT;
  wr.size = count;
  wr.key = key;
  wr.ptr = const_cast<void*>(value);

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "Put failed");
    return 0;
  } else {
    return wr.size;
  }
}

Size GAlloc::Get(uint64_t key, void* value) {
  WorkRequest wr { };
  wr.op = GET;
  wr.key = key;
  wr.ptr = value;

  if (wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "Get failed");
    return 0;
  } else {
    return wr.size;
  }
}

#ifdef DHT
int GAlloc::HTable(void* addr) {
  WorkRequest wr{};
  wr.op = GET_HTABLE;
  wr.addr = (GAddr)addr;
  if (wh->SendRequest(&wr)) {
    return -1;
  } else {
    return 0;
  }
}
#endif

GAlloc::~GAlloc() {
  delete wh;
}
