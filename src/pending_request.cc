// Copyright (c) 2018 The GAM Authors 


#include <cstring>
#include <utility>
#include <queue>
#include "rdma.h"
#include "worker.h"
#include "anet.h"
#include "log.h"
#include "ae.h"
#include "client.h"
#include "util.h"
#include "structure.h"
#include "ae.h"
#include "tcp.h"
#include "slabs.h"
#include "zmalloc.h"
#include "kernel.h"
#include "chars.h"

void Worker::ProcessPendingRead(Client* cli, WorkRequest* wr) {
  epicAssert(wr->parent);
  epicAssert(
      (IsLocal(wr->addr) && wr->op == FETCH_AND_SHARED)
          || (!IsLocal(wr->addr) && wr->op == READ));
  //parent request is from local app or remote worker
  WorkRequest* parent = wr->parent;
  CacheLine* cline = nullptr;
  DirEntry* entry = nullptr;
  GAddr blk = TOBLOCK(wr->addr);
#ifndef SELECTIVE_CACHING
  epicAssert(blk == wr->addr);
#endif

  parent->lock();
  if (wr->flag & CACHED) {
    epicAssert(!IsLocal(wr->addr));
    cache.lock(blk);
    cline = cache.GetCLine(wr->addr);
    epicAssert(cline);
  } else if (IsLocal(wr->addr)) {
    directory.lock(ToLocal(wr->addr));
    entry = directory.GetEntry(ToLocal(wr->addr));
    epicAssert(entry);
  } else {
    epicLog(LOG_WARNING, "shouldn't happen");
    epicAssert(false);
  }

  if (!(wr->flag & LOCKED)) {
    GAddr pend = GADD(parent->addr, parent->size);
    GAddr end = GADD(wr->addr, wr->size);
    GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
    void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
    void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
    Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
    memcpy(ls, cs, len);
  }

  //update the cache or directory states
  if (!(wr->flag & REPEATED)) {
    if ((wr->flag & CACHED)) {  //read is issued by the cache (remote memory)
      epicAssert(wr->op == READ);
      epicAssert(!IsLocal(wr->addr));
#ifdef SELECTIVE_CACHING
      if(wr->flag & NOT_CACHE) {
        cache.ToNotCache(cline);
      } else {
        cache.ToShared(cline);
      }
#else
      cache.ToShared(cline);
#endif
    } else if (IsLocal(wr->addr)) {  //read is issued by local worker (local memory)
      epicAssert(wr->op == FETCH_AND_SHARED);
      directory.ToShared(entry, Gnullptr);
    } else {
      epicLog(LOG_WARNING, "unexpected!!!");
    }

    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
  }

  if (wr->flag & LOCKED) {  //RLOCK
    epicAssert(
        !(wr->flag & NOT_CACHE) && wr->addr == blk && wr->size == BLOCK_SIZE);
    epicAssert(
        RLOCK == parent->op && 1 == parent->counter && 0 == parent->size);
    if (wr->flag & CACHED) {  //RLOCK is issued by the cache (remote memory)
      epicAssert(wr->ptr == cline->line);
      epicAssert(!IsLocal(wr->addr));
      int ret = cache.RLock(cline, parent->addr);
      epicAssert(!ret);  //first rlock must be successful
    } else if (IsLocal(wr->addr)) {  //RLock is issued by local worker (local memory)
      epicAssert(ToLocal(wr->addr) == wr->ptr);
      int ret;
      if (entry) {
        ret = directory.RLock(entry, ToLocal(parent->addr));
      } else {
        ret = directory.RLock(ToLocal(parent->addr));  //the dir entry may be deleted
      }
      epicAssert(!ret);  //first rlock must be successful
    } else {
      epicLog(LOG_WARNING, "unexpected!!!");
    }
  }

  if (wr->flag & CACHED) {
    epicAssert(!IsLocal(wr->addr));
    cache.unlock(blk);
  } else if (IsLocal(wr->addr)) {
    directory.unlock(ToLocal(wr->addr));
  } else {
    epicLog(LOG_WARNING, "shouldn't happen");
    epicAssert(false);
  }

  if (--parent->counter == 0) {  //read all the data
    parent->unlock();
    Notify(parent);
  } else {
    parent->unlock();
  }

  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingReadForward(Client* cli, WorkRequest* wr) {
#ifdef SELECTIVE_CACHING
  epicAssert(!(wr->flag & NOT_CACHE));
#endif
  epicAssert(wr->parent);
  epicAssert(IsLocal(wr->addr));  //I'm the home node
  //parent request is from local node
  WorkRequest* parent = wr->parent;
  void* laddr = ToLocal(wr->addr);

  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  directory.ToShared(entry, Gnullptr);
  directory.ToShared(entry, FindClientWid(wr->pwid)->ToGlobal(parent->ptr));
  directory.unlock(laddr);
  //pending_works.erase(wr->id);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);

  ProcessToServeRequest(wr);
  delete wr;
  delete parent;
  wr = nullptr;
  parent = nullptr;
}

void Worker::ProcessPendingWrite(Client* cli, WorkRequest* wr) {
#ifdef SELECTIVE_CACHING
  wr->addr = TOBLOCK(wr->addr);
#endif
  epicAssert(
      (wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY)
          xor IsLocal(wr->addr));
  WorkRequest* parent;
  CacheLine* cline = nullptr;
  DirEntry* entry = nullptr;
  parent = wr->parent;
  parent->lock();
  if (wr->flag & CACHED) {
    epicAssert(!IsLocal(wr->addr));
    cache.lock(wr->addr);
    cline = cache.GetCLine(wr->addr);
    epicAssert(cline);
  } else if (IsLocal(wr->addr)) {
    directory.lock(ToLocal(wr->addr));
    entry = directory.GetEntry(ToLocal(wr->addr));
    epicAssert(entry);
  } else {
    epicLog(LOG_WARNING, "shouldn't happen");
    epicAssert(false);
  }
  wr->lock();

  if (!(wr->flag & REPEATED) && !(wr->flag & REQUEST_DONE)) {
    if (WID(wr->addr) == cli->GetWorkerId()) {  //from home node, Case 4
      wr->counter++;
    } else {
      wr->counter--;
    }
  }

  //failed case
  if (wr->status) {  //failed from one of the responders
    epicLog(LOG_INFO, "failed case after-processing");
    epicAssert((wr->flag & LOCKED) && (wr->flag & TRY_LOCK));
    epicAssert(wr->status == LOCK_FAILED);
    epicAssert(wr->op != FETCH_AND_INVALIDATE);
    if ((wr->flag & CACHED)) {  //write is issued by the cache (remote memory)
      epicAssert(wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY);
      epicAssert(!IsLocal(wr->addr));
    } else if (IsLocal(wr->addr)) {  //write is issued by local worker (local memory)
      epicAssert(wr->op == INVALIDATE);
      epicAssert(wr->ptr == ToLocal(wr->addr));
      epicAssert(directory.GetState(entry) == DIR_TO_UNSHARED);
      directory.Remove(entry, cli->GetWorkerId());
      epicAssert(directory.GetState(entry) != DIR_UNSHARED);  //not possible to erase the entry
    } else {
      epicLog(LOG_WARNING, "unexpected");
      epicAssert(false);
    }

    if (wr->counter == 0) {
      epicLog(LOG_INFO, "failed case final-processing");
      //undo the directory/cache changes
      if (WRITE == wr->op) {
        cache.ToInvalid(cline);
      } else if (WRITE_PERMISSION_ONLY == wr->op) {
        cache.UndoShared(cline);
      } else if (FETCH_AND_INVALIDATE == wr->op) {
        epicAssert(wr->ptr == ToLocal(wr->addr));
        directory.UndoDirty(entry);
      } else {  //INVALIDATE
        epicAssert(wr->ptr == ToLocal(wr->addr));
        directory.UndoShared(entry);
      }

      wr->unlock();
      //unlock before process other requests
      if (wr->flag & CACHED) {
        epicAssert(!IsLocal(wr->addr));
        cache.unlock(wr->addr);
      } else if (IsLocal(wr->addr)) {
        directory.unlock(ToLocal(wr->addr));
      } else {
        epicLog(LOG_WARNING, "shouldn't happen");
        epicAssert(false);
      }

      --wr->parent->counter;
      epicAssert(wr->parent->counter == 0);  //lock is guaranteed to be only one block
      parent->unlock(); // unlock earlier
      // Notify() should be called in the very last after all usage of parent,
      // since the app thread may exit the function and release the memory of parent
      Notify(wr->parent);
      wr->parent = nullptr;

      epicAssert(!wr->next);
      ProcessToServeRequest(wr);
      //pending_works.erase(wr->id);
      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
      delete wr;
      wr = nullptr;
    } else {
      wr->unlock();
      parent->unlock(); // unlock earlier
      if (wr->flag & CACHED) {
        epicAssert(!IsLocal(wr->addr));
        cache.unlock(wr->addr);
      } else if (IsLocal(wr->addr)) {
        directory.unlock(ToLocal(wr->addr));
      } else {
        epicLog(LOG_WARNING, "shouldn't happen");
        epicAssert(false);
      }
    }
    // parent->unlock(); // @wentian: originally here
    return;
  }

  if ((wr->flag & CACHED)) {  //write is issued by the cache (remote memory)
    epicAssert(wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY);
    epicAssert(!IsLocal(wr->addr));
    epicAssert(!IsLocal(wr->addr));
  } else if (IsLocal(wr->addr)) {  //write is issued by local worker (local memory)
    epicAssert(wr->ptr == ToLocal(wr->addr));
    epicAssert(directory.GetState(entry) == DIR_TO_UNSHARED);
    directory.Remove(entry, cli->GetWorkerId());
  } else {
    epicLog(LOG_WARNING, "unexpected");
    epicAssert(false);
  }

  //normal process below
  epicLog(LOG_DEBUG, "wr->counter after = %d", wr->counter.load());
  epicAssert(parent);
  if (wr->counter == 0 || (wr->flag & REQUEST_DONE)) {

#ifdef SELECTIVE_CACHING
    if(!(wr->flag & NOT_CACHE)) {
#endif

    if (!(wr->flag & LOCKED)) {
      GAddr pend = GADD(parent->addr, parent->size);
      GAddr end = GADD(wr->addr, wr->size);
      GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
      void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
      void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
      Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
      //we blindly copy the data again
      //as while we are waiting for the reply,
      //there may be a race causes the current op to be canceled or renamed (WRITE_PERMISSION_ONLY to WRITE)
#ifdef GFUNC_SUPPORT
      if (wr->flag & GFUNC) {
        epicAssert(wr->gfunc);
        epicAssert(TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size-1)));
        void* laddr = cs;
        wr->gfunc(laddr, wr->arg);
      } else {
#endif
        memcpy(cs, ls, len);
#ifdef GFUNC_SUPPORT
      }
#endif
    }

#ifdef SELECTIVE_CACHING
  }
#endif

    if (!(wr->flag & REPEATED)) {
      if ((wr->flag & CACHED)) {  //write is issued by the cache (remote memory)
        epicAssert(wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY);
        epicAssert(!IsLocal(wr->addr));
#ifdef SELECTIVE_CACHING
        if(wr->flag & NOT_CACHE) {
          epicAssert(wr->op != WRITE_PERMISSION_ONLY);
          cache.ToNotCache(cline, true);
        } else {
          // do logging here
          // logWrite(cline->addr, BLOCK_SIZE, cline->line);
          cache.ToDirty(cline);
        }
#else
        // do logging here
        // logWrite(cline->addr, BLOCK_SIZE, cline->line);
        cache.ToDirty(cline);
#endif
      } else if (IsLocal(wr->addr)) {  //write is issued by local worker (local memory)
        directory.ToUnShared(entry);
      } else {
        epicLog(LOG_WARNING, "shouldn't happen");
        epicAssert(false);
      }

      //clear the pending structures
      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
    }

    if (wr->flag & LOCKED) {  //WLOCK
      epicLog(LOG_DEBUG, "parent->op = %d, parent->counter = %d", parent->op,
              parent->counter.load());
      epicAssert(WLOCK == parent->op && 1 == parent->counter);
    }

    //TODO: we should process the to_serve requests and pending requests first
    //then process the fenced requests
    //for now, it's ok since write will prevent fenced requests to be processed
    //and read is blocking
    bool notify = false;
    epicLog(LOG_DEBUG, "parent->counter = %d", parent->counter.load());
    if (--parent->counter == 0) {  //write all the blocks
      if (WLOCK == parent->op) {
        if (wr->flag & CACHED) {
          epicAssert(!IsLocal(wr->addr));
          epicAssert(wr->ptr == cline->line);
          if (cache.WLock(cline, parent->addr)) {  //lock failed
            epicAssert(
                wr->op == WRITE_PERMISSION_ONLY
                    && cache.IsRLocked(cline, parent->addr));  //must be shared locked before and now
            epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
            AddToServeLocalRequest(wr->addr, parent);
          } else {
            notify = true;
          }
        } else if (IsLocal(wr->addr)) {
          epicAssert(ToLocal(wr->addr) == wr->ptr);
          int ret;
          if (entry) {
            ret = directory.WLock(entry, ToLocal(parent->addr));
          } else {
            ret = directory.WLock(ToLocal(parent->addr));
          }
          if (ret) {  //lock failed
            epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
            AddToServeLocalRequest(wr->addr, parent);
          } else {
            notify = true;
          }
        } else {
          epicLog(LOG_WARNING, "unexpected!!!");
        }
      } else {
        notify = true;
      }
    }

    wr->unlock();
    if (wr->flag & CACHED) {
      epicAssert(!IsLocal(wr->addr));
      cache.unlock(wr->addr);
    } else if (IsLocal(wr->addr)) {
      directory.unlock(ToLocal(wr->addr));
    } else {
      epicLog(LOG_WARNING, "shouldn't happen");
      epicAssert(false);
    }
    parent->unlock();

    if (notify)
      Notify(parent);

    ProcessToServeRequest(wr);
    delete wr;
    wr = nullptr;
  } else {
    wr->unlock();
    //don't forget to unlock
    if (wr->flag & CACHED) {
      epicAssert(!IsLocal(wr->addr));
      cache.unlock(wr->addr);
    } else if (IsLocal(wr->addr)) {
      directory.unlock(ToLocal(wr->addr));
    } else {
      epicLog(LOG_WARNING, "shouldn't happen");
      epicAssert(false);
    }
    parent->unlock();
  }
}

void Worker::ProcessPendingWriteForward(Client* cli, WorkRequest* wr) {
  epicAssert(wr->parent);
  epicAssert(IsLocal(wr->addr));  //I'm the home node
  WorkRequest* parent = wr->parent;
  epicAssert(wr->pid == parent->id);

  void* laddr = ToLocal(wr->addr);
  Client* lcli = FindClientWid(wr->pwid);
  epicAssert(BLOCK_ALIGNED(wr->addr));

#ifdef SELECTIVE_CACHING
  if(wr->flag & NOT_CACHE) {
    directory.lock(laddr);
#ifdef GFUNC_SUPPORT
    if(wr->flag & GFUNC) {
      epicAssert(parent->gfunc);
      epicAssert(TOBLOCK(parent->addr) == TOBLOCK(GADD(parent->addr, parent->size-1)));
      void* laddr = ToLocal(parent->addr);
      wr->gfunc(laddr, wr->arg);
    } else {
#endif
      memcpy(ToLocal(parent->addr), parent->ptr, parent->size);
#ifdef GFUNC_SUPPORT
    }
#endif
    directory.ToUnShared(laddr);
    directory.unlock(laddr);

    parent->op = WRITE_REPLY;
    parent->status = SUCCESS;
    SubmitRequest(lcli, parent);
  } else {
#endif

  directory.lock(laddr);
  logOwner(lcli->GetWorkerId(), wr->addr);
  directory.ToDirty(laddr, lcli->ToGlobal(parent->ptr));
  directory.unlock(laddr);

  //TOOD: add completion check
  lcli->WriteWithImm(nullptr, nullptr, 0, wr->pid);  //ack the ownership change

#ifdef SELECTIVE_CACHING
}
#endif

  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  ProcessToServeRequest(wr);
  delete wr;
  delete parent;
  wr = nullptr;
  parent = nullptr;
}

void Worker::ProcessPendingEvictDirty(Client* cil, WorkRequest* wr) {
  cache.to_evicted--;
  cache.lock(wr->addr);
  cache.ToInvalid(wr->addr);
  cache.unlock(wr->addr);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingInvalidateForward(Client* cli, WorkRequest* wr) {
  WorkRequest* parent = wr->parent;
  epicAssert(parent);
  epicAssert(TOBLOCK(parent->addr) == wr->addr);
  epicAssert(wr->size == BLOCK_SIZE);
  parent->lock();

  void* laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  wr->lock();

  epicAssert(IsLocal(wr->addr));
  epicLog(LOG_DEBUG, "wr->counter before = %d", wr->counter.load());
  epicAssert(!(wr->flag & REPEATED) && !(wr->flag & REQUEST_DONE));
  wr->counter--;
  epicLog(LOG_DEBUG, "wr->counter after = %d", wr->counter.load());

  DirEntry* entry = directory.GetEntry(ToLocal(wr->addr));

  epicAssert(directory.GetState(entry) == DIR_TO_DIRTY);
  directory.Remove(entry, cli->GetWorkerId());

  //failed case
  if (wr->status) {  //failed from one of the responders
    epicLog(LOG_INFO, "INVALIDATE_FORWARD: failed case after-processing");
    epicAssert((wr->flag & LOCKED) && (wr->flag & TRY_LOCK));
    epicAssert(wr->status == LOCK_FAILED);
    epicAssert(directory.GetState(entry) != DIR_UNSHARED);  //not possible to erase the entry

    if (wr->counter == 0) {
      epicLog(LOG_INFO, "INVALIDATE_FORWARD: failed case final-processing");
      void* laddr = ToLocal(wr->addr);
      epicAssert(directory.GetState(entry) == DIR_TO_DIRTY);
      //undo the directory/cache changes
      directory.UndoShared(entry);

      wr->unlock();
      directory.unlock(ToLocal(wr->addr));

      Client* pcli = FindClientWid(wr->pwid);
      parent->status = wr->status;  //put the error status
      parent->op = WRITE_REPLY;
      //comment below as counter is not initialized and not used in the write/invalidforward
      //parent->counter--;
      //epicAssert(parent->counter == 0);
      SubmitRequest(pcli, parent);
      parent->unlock();
      delete parent;
      parent = nullptr;
      wr->parent = nullptr;

      ProcessToServeRequest(wr);
      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
      delete wr;
      wr = nullptr;
      //parent has been already deleted when we receive failed response the first time
    } else {
      wr->unlock();
      directory.unlock(ToLocal(wr->addr));
      parent->unlock();
    }
    return;
  }

  //normal process below
  if (wr->counter == 0) {
    Client* lcli = FindClientWid(wr->pwid);

#ifdef SELECTIVE_CACHING
    if(wr->flag & NOT_CACHE) {
#ifdef GFUNC_SUPPORT
      if(wr->flag & GFUNC) {
        epicAssert(parent->gfunc);
        epicAssert(TOBLOCK(parent->addr) == TOBLOCK(GADD(parent->addr, parent->size-1)));
        void* laddr = ToLocal(parent->addr);
        wr->gfunc(laddr, wr->arg);
      } else {
#endif
        memcpy(ToLocal(parent->addr), parent->ptr, parent->size);
#ifdef GFUNC_SUPPORT
      }
#endif
    } else {
#endif

    if (WRITE == parent->op) {
      lcli->Write(parent->ptr, laddr, parent->size);
      epicLog(LOG_DEBUG, "write the data (size = %ld) to destination",
              parent->size);
    } else {  //WRITE_PERMISSION_ONLY
      epicAssert(WRITE_PERMISSION_ONLY == parent->op);
      //deadlock: one node (Node A) wants to update its cache from shared to dirty,
      //but at the same time, the home nodes invalidates all its shared copy
      //(due to a local write, or remote write after local/remote read)
      //currently, dir_state == DIR_UNSHARED
      //which means that the shared list doesn't contain the requesting node A.
      //solution: Node A acts as it is still a shared copy so that the invalidation can completes,
      //after which, home node processes the pending list
      //and change the processing from WRITE_PERMISSION_ONLY to WRITE
      if (DIR_UNSHARED == directory.GetState(entry)) {
        lcli->Write(parent->ptr, laddr, parent->size);
        epicLog(LOG_INFO, "deadlock detected");
        epicLog(LOG_DEBUG, "write the data to destination");
      }
    }

    // logging a ownership
    logOwner(lcli->GetWorkerId(), wr->addr);

    if (entry) {
      directory.ToDirty(entry, lcli->ToGlobal(parent->ptr));  //entry should be null
    } else {
      directory.ToDirty(laddr, lcli->ToGlobal(parent->ptr));  //entry should be null
    }

#ifdef SELECTIVE_CACHING
  }
#endif

    wr->unlock();
    directory.unlock(laddr);

    parent->op = WRITE_REPLY;
    parent->status = SUCCESS;
    parent->counter = 0;
    SubmitRequest(lcli, parent);
    parent->unlock();

    //clear the pending structures
    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
    ProcessToServeRequest(wr);
    delete wr;
    delete parent;
    wr = nullptr;
    parent = nullptr;
  } else {
    wr->unlock();
    directory.unlock(laddr);
    parent->unlock();
  }
}

void Worker::ProcessPendingRequest(Client* cli, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "process pending request %d from worker %d", wr->op,
          cli->GetWorkerId());
  switch (wr->op) {
    case READ:
    case FETCH_AND_SHARED: {
      ProcessPendingRead(cli, wr);
      break;
    }
    case READ_FORWARD: {
      ProcessPendingReadForward(cli, wr);
      break;
    }
    case FETCH_AND_INVALIDATE:
    case INVALIDATE:
    case WRITE:
    case WRITE_PERMISSION_ONLY: {
      ProcessPendingWrite(cli, wr);
      break;
    }
    case WRITE_FORWARD:  //Case 4 in home node
    {
      ProcessPendingWriteForward(cli, wr);
      break;
    }
    case WRITE_BACK: {
      ProcessPendingEvictDirty(cli, wr);
      break;
    }
    case INVALIDATE_FORWARD: {
      ProcessPendingInvalidateForward(cli, wr);
      break;
    }
    default:
      epicLog(LOG_WARNING, "unrecognized work request %d", wr->op);
      exit(-1);
      break;
  }
}

/*
 * callback function for locally initiated asynchronous request
 */
void Worker::ProcessRequest(Client* cli, unsigned int work_id) {
#ifdef NOCACHE
  epicLog(LOG_WARNING, "shouldn't come here");
  return;
#endif
  epicLog(LOG_DEBUG, "callback function work_id = %u, reply from %d", work_id,
          cli->GetWorkerId());
  WorkRequest* wr = GetPendingWork(work_id);
  epicAssert(wr);
  epicAssert(wr->id == work_id);
  ProcessPendingRequest(cli, wr);
}
