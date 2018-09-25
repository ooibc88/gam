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
#include "util.h"

#ifdef DHT
int Worker::ProcessLocalHTable(WorkRequest* pwr) {
    pwr->id = this->GetWorkPsn();

    uint64_t mem = this->FindClientWid(GetWorkerId())->GetTotalMem();
    if (mem > 0)
        this->ProcessHTableReply(NULL, pwr);

    bool local_only = true;
    for (auto& entry: widCliMapWorker) {
        if (entry.first != this->GetWorkerId()) {
            this->SubmitRequest(entry.second, pwr);
            local_only = false;
        }
    }

    if (!local_only) {
        AddToPending(pwr->id, pwr);
        return REMOTE_REQUEST;
    } else return SUCCESS;
}
#endif // DHT

int Worker::ProcessLocalMalloc(WorkRequest* wr) {
  epicAssert(!(wr->flag & ASYNC));
  if ((wr->flag & REMOTE) || (wr->addr && !IsLocal(wr->addr))) {  //remote alloc
    Client* cli = GetClient(wr->addr);
    if (!cli) {
      //wr->status = ALLOC_ERROR;
      epicLog(LOG_WARNING,
              "there is no remote worker, we allocate locally instead");
    } else {
      cli->SetMemStat(cli->GetTotalMem(), cli->GetFreeMem() - wr->size);
      SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
      return REMOTE_REQUEST;
    }
  } else if (wr->flag & RANDOM) {
    size_t size = GetWorkersSize();
    static unsigned int seed = GetWorkerId();
    int i;
    Client* cli = nullptr;
    for (i = 0; i < size; i++) {
      cli = nullptr;
      int workid = GetRandom(0, size, &seed);
      epicLog(LOG_WARNING, "workid = %d, size = %d", workid, size);
      auto it = widCliMapWorker.begin();
      while (workid--)
        it++;
      epicAssert(it != widCliMapWorker.end());
      cli = it->second;
      if (cli->GetWorkerId() == GetWorkerId()) {  //local allocation
        cli = nullptr;
        break;
      }
      if (wr->size <= cli->GetFreeMem()) {
        break;
      }
    }
    if (i == size) {  //cannot find a suitable client, try remote scheme again (rare case)
      cli = GetClient(wr->addr);  //wr->addr = null
    }
    if (cli) {
      cli->SetMemStat(cli->GetTotalMem(), cli->GetFreeMem() - wr->size);
      SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
      return REMOTE_REQUEST;
    }
  }
  //local alloc
  //we reserve a minimum conf->cache_th size for cache
  if (cache.GetUsedBytes() + sb.get_avail() < conf->size * conf->cache_th) {
    Client* cli = GetClient();
    cli->lock();
    if (cli) {
      epicLog(LOG_DEBUG, "allocate remotely at worker %d", cli->GetWorkerId());
      Size free = cli->GetFreeMem();
      cli->SetMemStat(cli->GetTotalMem(), free - wr->size);  //update memory stats
      cli->unlock();
      SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
      return REMOTE_REQUEST;
    } else {
      //no remote worker, we have no choice expect allocate locally
      epicLog(LOG_WARNING, "local memory pressure, but there is no remote worker");
      cli->unlock();
    }
  }

  void* addr;
  if (wr->flag & ALIGNED) {
    addr = sb.sb_aligned_malloc(wr->size);
    epicAssert((uint64_t)addr % BLOCK_SIZE == 0);
  } else {
    addr = sb.sb_malloc(wr->size);
    epicLog(LOG_DEBUG, "allocate addr at %lx", addr);
  }
  //FIXME: remove below
  memset(addr, 0, wr->size);
  if (addr) {
    wr->addr = TO_GLOB(addr, base, GetWorkerId());
    wr->status = SUCCESS;
    ghost_size += wr->size;
    if (abs(ghost_size.load()) > conf->ghost_th)
      SyncMaster();
  } else {
    wr->status = ALLOC_ERROR;
  }

#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

//FIXME: check whether other nodes are sharing this data
//issue a write request first, and then process the free
int Worker::ProcessLocalFree(WorkRequest* wr) {
  epicAssert(!(wr->flag & ASYNC));
  //TODO: whether need to invalidate the cached copies
  //don't need to invalidate as other data co-located within the same block may be still in use.
  epicAssert(wr->addr);
  if (IsLocal(wr->addr)) {
    void* addr = ToLocal(wr->addr);
    Size size = sb.sb_free(addr);
    ghost_size -= size;
    if (abs(ghost_size.load()) > conf->ghost_th)
      SyncMaster();
  } else {
    Client* cli = GetClient(wr->addr);
    if (!cli) {
      wr->status = ALLOC_ERROR;
    } else {
      SubmitRequest(cli, wr);
    }
  }
#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalMFence(WorkRequest* wr) {
  epicAssert(!(wr->flag & FENCE));
  Fence* fence = fences_.at(wr->fd);
  fence->lock();
  if (unlikely(IsFenced(fence, wr))) {
    epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
            fence->mfenced, fence->sfenced, fence->pending_works.size());
    AddToFence(fence, wr);
    fence->unlock();
  } else {
    if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced!!, pending_writes = %d",
              fence->pending_writes.load());
    }
    fence->unlock();
#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
      /*
       * notify the app thread directly
       * this can only happen when the request can be fulfilled locally
       * or we don't need to wait for reply from remote node
       */
      if (Notify(wr)) {
        epicLog(LOG_WARNING, "cannot wake up the app thread");
      }
#ifdef MULTITHREAD
    }
#endif

  }
  return SUCCESS;
}

int Worker::ProcessLocalSFence(WorkRequest* wr) {
  epicAssert(!(wr->flag & FENCE));
  //TODO: add the sfence support
  epicLog(LOG_WARNING, "SFENCE is not supported for now!");
  Fence* fence = fences_.at(wr->fd);
  fence->lock();
  if (IsFenced(fence, wr)) {
    epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
            fence->mfenced, fence->sfenced, wr->op);
    AddToFence(fence, wr);
    fence->unlock();
  } else {
    if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->sfenced = true;
      epicLog(LOG_DEBUG, "sfenced!");
    }
    fence->unlock();
#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
      /*
       * notify the app thread directly
       * this can only happen when the request can be fulfilled locally
       * or we don't need to wait for reply from remote node
       */
      if (Notify(wr)) {
        epicLog(LOG_WARNING, "cannot wake up the app thread");
      }
#ifdef MULTITHREAD
    }
#endif
  }
  return SUCCESS;
}

#ifdef NOCACHE
int Worker::ProcessLocalRead(WorkRequest* wr) {
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));
  epicAssert(wr->size < MAX_REQUEST_SIZE);
  if(!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    fence->lock();
    if(unlikely(IsMFenced(fence, wr))) {
      AddToFence(fence, wr);
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d", fence->mfenced, fence->sfenced, wr->op);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }

  if(likely(IsLocal(wr->addr))) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    GAddr end = GADD(start, wr->size);
    if(TOBLOCK(end-1) != start_blk) {
      epicLog(LOG_INFO, "read/write split to multiple blocks");
    }
    for(GAddr i = start_blk; i < end;) {
      epicAssert(!(wr->flag & COPY) || ((wr->flag & COPY) && (wr->flag & ASYNC)));
      GAddr nextb = BADD(i, 1);
      void* laddr = ToLocal(i);

      directory.lock(laddr);
      GAddr gs = i > start ? i : start;
      void* ls = (void*)((ptr_t)wr->ptr + GMINUS(gs, start));
      int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
      memcpy(ls, ToLocal(gs), len);
      directory.unlock(laddr);
      i = nextb;
    }

  } else {
    Client* cli = GetClient(wr->addr);
    SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
    return REMOTE_REQUEST;
  }
#ifdef MULTITHREAD
  if(wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if(Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalWrite(WorkRequest* wr) {
  epicAssert(wr->addr);
  epicAssert(wr->size < MAX_REQUEST_SIZE);
  epicAssert(wr->flag & ASYNC);
  Fence* fence = fences_.at(wr->fd);
  if(!(wr->flag & FENCE)) {
    fence->lock();
    if(unlikely(IsFenced(fence, wr))) {
      epicLog(LOG_DEBUG, "fenced(mfenced = %d, sfenced = %d): %d", fence->mfenced, fence->sfenced, wr->op);
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if((wr->flag & ASYNC) && !(wr->flag & TO_SERVE)) {
    //fences_[wr->fd].pending_writes++;
    fence->pending_writes++;
    epicLog(LOG_DEBUG, "Local: one more pending write");
  }
  if(likely(IsLocal(wr->addr))) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    GAddr end = GADD(start, wr->size);
    if(TOBLOCK(end-1) != start_blk) {
      epicLog(LOG_INFO, "read/write split to multiple blocks");
    }
    for(GAddr i = start_blk; i < end;) {
      epicAssert(!(wr->flag & COPY) || ((wr->flag & COPY) && (wr->flag & ASYNC)));
      GAddr nextb = BADD(i, 1);
      void* laddr = ToLocal(i);

      directory.lock(laddr);
      GAddr gs = i > start ? i : start;
      void* ls = (void*)((ptr_t)wr->ptr + GMINUS(gs, start));
      int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
      logWrite(gs, len, ls);
      memcpy(ToLocal(gs), len, ls);
      directory.unlock(laddr);
      i = nextb;
    }
  } else {
    Client* cli = GetClient(wr->addr);
    if(wr->flag & ASYNC) {
      if(!wr->IsACopy()) {
        wr = wr->Copy();
      }
    }
    SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
    return REMOTE_REQUEST;
  }
#ifdef MULTITHREAD
  if(wr->flag & ASYNC || wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if(Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalRLock(WorkRequest* wr) {
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));
  //epicAssert(!(wr->flag & FENCE));
  if(!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    //if(fence->mfenced || fence->sfenced) {
    fence->lock();
    if(IsFenced(fence, wr)) {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d", fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    } else if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from RLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if(IsLocal(wr->addr)) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    void* laddr = ToLocal(start_blk);

    directory.lock(laddr);
    int ret = directory.RLock(ToLocal(wr->addr));
    if(ret) {  //fail to lock
      epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
      if(wr->flag & TRY_LOCK) {
        wr->status = LOCK_FAILED;
      } else {
        //to_serve_local_requests[start_blk].push(wr);
        AddToServeLocalRequest(start_blk, wr);
        directory.unlock(laddr);
        return IN_TRANSITION;
      }
    }
    directory.unlock(laddr);
  } else {
    Client* cli = GetClient(wr->addr);
    SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
    return REMOTE_REQUEST;
  }
#ifdef MULTITHREAD
  if(wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if(Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalWLock(WorkRequest* wr) {
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));
  //epicAssert(!(wr->flag & FENCE));
  if(!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    //if(fence->mfenced || fence->sfenced) {
    fence->lock();
    if(IsFenced(fence, wr)) {
      //fence->pending_works.push(wr);
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d", fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    } else if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from WLOCK!");
      //fence->pending_works.push(wr);
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if(IsLocal(wr->addr)) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    void* laddr = ToLocal(start_blk);

    directory.lock(laddr);
    int ret = directory.WLock(ToLocal(wr->addr));
    if(ret) {  //failed to lock
      epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
      if(wr->flag & TRY_LOCK) {
        wr->status = LOCK_FAILED;
      } else {
        //to_serve_local_requests[start_blk].push(wr);
        AddToServeLocalRequest(start_blk, wr);
        directory.unlock(laddr);
        return IN_TRANSITION;
      }
    }
    directory.unlock(laddr);
  } else {
    Client* cli = GetClient(wr->addr);
    SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
    return REMOTE_REQUEST;
  }
#ifdef MULTITHREAD
  if(wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if(Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalUnLock(WorkRequest* wr) {
  if(!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    fence->lock();
    if(IsFenced(fence, wr)) {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d", fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    } else if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from UNLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }

  if(IsLocal(wr->addr)) {
    GAddr start_blk = TOBLOCK(wr->addr);
    void* laddr = ToLocal(start_blk);
    directory.lock(laddr);
    directory.UnLock(ToLocal(wr->addr));
    directory.unlock(laddr);
  } else {
    Client* cli = GetClient(wr->addr);
#ifdef ASYNC_UNLOCK
    SubmitRequest(cli, wr);  //no need for reply
#else
    SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);  //no need for reply
#endif
    return REMOTE_REQUEST;
  }
  ProcessToServeRequest(wr);

#ifdef MULTITHREAD
  if(wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if(Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

#else //if not NOCACHE
int Worker::ProcessLocalRead(WorkRequest* wr) {
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));

  if (!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    fence->lock();
    if (unlikely(IsMFenced(fence, wr))) {
      AddToFence(fence, wr);
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }

  if (likely(IsLocal(wr->addr))) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    GAddr end = GADD(start, wr->size);
    wr->lock();
    /*
     * we increase it by 1 before we push to the to_serve_local_request queue
     * so we have to decrease by 1 again
     */
    if (wr->flag & TO_SERVE) {
      wr->counter--;
    }
    for (GAddr i = start_blk; i < end;) {
      GAddr nextb = BADD(i, 1);
      void* laddr = ToLocal(i);

      directory.lock(laddr);
      DirEntry* entry = directory.GetEntry(laddr);
      DirState s = directory.GetState(entry);
      if (unlikely(directory.InTransitionState(s))) {
        epicLog(LOG_INFO, "directory in transition state when local read %d",
                s);
        //we increase the counter in case
        //we false call Notify()
        wr->counter++;
        AddToServeLocalRequest(i, wr);
        directory.unlock(laddr);
        wr->unlock();
        wr->is_cache_hit_ = false;
        return IN_TRANSITION;
      }

      if (unlikely(s == DIR_DIRTY)) {
        WorkRequest* lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        GAddr rc = directory.GetSList(entry).front();  //only one worker is updating this line
        Client* cli = GetClient(rc);
        lwr->op = FETCH_AND_SHARED;
        lwr->addr = i;
        lwr->size = BLOCK_SIZE;
        lwr->ptr = laddr;
        lwr->parent = wr;
        wr->counter++;
        wr->is_cache_hit_ = false;
        //intermediate state
        epicAssert(s != DIR_TO_SHARED);
        epicAssert(!directory.IsBlockLocked(entry));
        directory.ToToShared(entry, rc);
        SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
      } else {
        GAddr gs = i > start ? i : start;
        void* ls = (void*) ((ptr_t) wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        memcpy(ls, ToLocal(gs), len);
      }
      directory.unlock(laddr);
      i = nextb;
    }
    if (unlikely(wr->counter)) {
      wr->unlock();
      return REMOTE_REQUEST;
    } else {
      wr->unlock();
    }
  } else {
    int ret = cache.Read(wr);
    if (ret)
      return REMOTE_REQUEST;
  }


#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  } else {
    /**
     * In this case, the read request is running in the app thread and
     * is fulfilled in the first trial (i.e., * chache hit)
     */
    epicAssert(wr->is_cache_hit_);
    if (IsLocal(wr->addr)) {
        ++no_local_reads_;
        ++no_local_reads_hit_;
    } else {
        ++no_remote_reads_;
        ++no_remote_reads_hit_;
    }
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalWrite(WorkRequest* wr) {
  epicAssert(wr->addr);
  Fence* fence = fences_.at(wr->fd);
  if (!(wr->flag & FENCE)) {
    fence->lock();
    if (unlikely(IsFenced(fence, wr))) {
      epicLog(LOG_DEBUG, "fenced(mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if ((wr->flag & ASYNC) && !(wr->flag & TO_SERVE)) {
    fence->pending_writes++;
    epicLog(LOG_DEBUG, "Local: one more pending write");
  }
  if (likely(IsLocal(wr->addr))) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    GAddr end = GADD(start, wr->size);
    if (TOBLOCK(end-1) != start_blk) {
      epicLog(LOG_INFO, "read/write split to multiple blocks");
    }
    wr->lock();
    /*
     * we increase it by 1 before we push to the to_serve_local_request queue
     * so we have to decrease by 1 again
     */
    if (wr->flag & TO_SERVE) {
      wr->counter--;
    }
    for (GAddr i = start_blk; i < end;) {
      epicAssert(
          !(wr->flag & COPY) || ((wr->flag & COPY) && (wr->flag & ASYNC)));

      GAddr nextb = BADD(i, 1);
      void* laddr = ToLocal(i);

      directory.lock(laddr);
      DirEntry* entry = directory.GetEntry(laddr);
      DirState state = directory.GetState(entry);
      if (unlikely(directory.InTransitionState(state))) {
        epicLog(LOG_INFO, "directory in transition state when local write %d",
                state);
        //we increase the counter in case
        //we false call Notify()
        wr->counter++;
        wr->is_cache_hit_ = false;
        if (wr->flag & ASYNC) {
          if (!wr->IsACopy()) {
            wr->unlock();
            wr = wr->Copy();
            wr->lock();
          }
        }
        AddToServeLocalRequest(i, wr);
        directory.unlock(laddr);
        wr->unlock();
        return IN_TRANSITION;
      }

      /*
       * since we cannot guarantee that generating a completion indicates
       * the buf in the remote node has been updated (only means remote HCA received and acked)
       * (ref: http://lists.openfabrics.org/pipermail/general/2007-May/036615.html)
       * so we use Request/Reply mode even for DIR_SHARED invalidations
       * instead of direct WRITE or CAS to invalidate the corresponding cache line in remote node
       */
      if (state == DIR_DIRTY || state == DIR_SHARED) {
        list<GAddr>& shared = directory.GetSList(entry);
        WorkRequest* lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        lwr->op = state == DIR_DIRTY ? FETCH_AND_INVALIDATE : INVALIDATE;
        lwr->addr = i;
        lwr->size = BLOCK_SIZE;
        lwr->ptr = laddr;
        wr->is_cache_hit_ = false;
        if (wr->flag & ASYNC) {
          if (!wr->IsACopy()) {
            wr->unlock();
            wr = wr->Copy();
            wr->lock();
          }
        }
        lwr->parent = wr;
        lwr->id = GetWorkPsn();
        lwr->counter = shared.size();
        wr->counter++;
        epicAssert(state != DIR_TO_UNSHARED);
        epicAssert(
            (state == DIR_DIRTY && !directory.IsBlockLocked(entry))
                || (state == DIR_SHARED && !directory.IsBlockWLocked(entry)));
        directory.ToToUnShared(entry);
        //we move AddToPending before submit request
        //since it is possible that the reply comes back before we add to the pending list
        //if we AddToPending at last
        AddToPending(lwr->id, lwr);
        for (auto it = shared.begin(); it != shared.end(); it++) {
          Client* cli = GetClient(*it);
          epicLog(LOG_DEBUG, "invalidate (%d) cache from worker %d (lwr = %lx)",
                  lwr->op, cli->GetWorkerId(), lwr);
          SubmitRequest(cli, lwr);
          //lwr->counter++;
        }
      } else {
#ifdef GFUNC_SUPPORT
        if (wr->flag & GFUNC) {
          epicAssert(wr->gfunc);
          epicAssert(TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size-1)));
          epicAssert(i == start_blk);
          void* laddr = ToLocal(wr->addr);
          wr->gfunc(laddr, wr->arg);
        } else {
#endif
          GAddr gs = i > start ? i : start;
          void* ls = (void*) ((ptr_t) wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          memcpy(ToLocal(gs), ls, len);
          epicLog(LOG_DEBUG, "copy dirty data in advance");
#ifdef GFUNC_SUPPORT
        }
#endif
      }
      directory.unlock(laddr);
      i = nextb;
    }
    if (wr->counter) {
      wr->unlock();
      return REMOTE_REQUEST;
    } else {
      wr->unlock();
    }
  } else {
    int ret = cache.Write(wr);
    if (ret) {
      return REMOTE_REQUEST;
    }

    ++no_remote_writes_direct_hit_;
  }
#ifdef MULTITHREAD
  if (wr->flag & ASYNC || wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalRLock(WorkRequest* wr) {
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));
  //epicAssert(!(wr->flag & FENCE));
  if (!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    fence->lock();
    if (IsFenced(fence, wr)) {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    } else if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from RLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if (IsLocal(wr->addr)) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    void* laddr = ToLocal(start_blk);

    wr->lock();
    directory.lock(laddr);
    DirEntry* entry = directory.GetEntry(laddr);
    DirState state = directory.GetState(entry);
    if (directory.InTransitionState(state)) {
      epicLog(LOG_INFO, "directory in transition state when local read %d",
              state);
      AddToServeLocalRequest(start_blk, wr);
      directory.unlock(laddr);
      wr->unlock();
      return IN_TRANSITION;
    }
    if (state == DIR_DIRTY) {
      WorkRequest* lwr = new WorkRequest(*wr);
      lwr->counter = 0;
      GAddr rc = directory.GetSList(entry).front();  //only one worker is updating this line
      Client* cli = GetClient(rc);
      lwr->op = FETCH_AND_SHARED;
      lwr->addr = start_blk;
      lwr->size = BLOCK_SIZE;
      lwr->ptr = laddr;
      lwr->parent = wr;
      lwr->flag |= LOCKED;
      wr->counter++;
      //intermediate state
      epicAssert(state != DIR_TO_SHARED);
      epicAssert(!directory.IsBlockLocked(entry));
      directory.ToToShared(entry, rc);
      SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
    } else {
      int ret;
      if (entry) {
        ret = directory.RLock(entry, ToLocal(wr->addr));
      } else {
        ret = directory.RLock(ToLocal(wr->addr));
      }
      if (ret) {  //fail to lock
        epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
        if (wr->flag & TRY_LOCK) {
          wr->status = LOCK_FAILED;
        } else {
          AddToServeLocalRequest(start_blk, wr);
          directory.unlock(laddr);
          wr->unlock();
          return IN_TRANSITION;
        }
      }
    }
    if (wr->counter) {
      directory.unlock(laddr);
      wr->unlock();
      return REMOTE_REQUEST;
    } else {
      directory.unlock(laddr);
      wr->unlock();
    }
  } else {
    //if there are remote requests
    int ret = cache.RLock(wr);
    if (ret) {
      return REMOTE_REQUEST;
    }
  }
#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  } else {
    epicAssert(wr->is_cache_hit_);
    if (IsLocal(wr->addr)) {
        ++no_local_reads_;
        ++no_local_reads_hit_;
    } else {
        ++no_remote_reads_;
        ++no_remote_reads_hit_;
    }
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalWLock(WorkRequest* wr) {
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));
  if (!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    fence->lock();
    if (IsFenced(fence, wr)) {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    } else if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from WLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if (IsLocal(wr->addr)) {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    void* laddr = ToLocal(start_blk);

    wr->lock();
    directory.lock(laddr);
    DirEntry* entry = directory.GetEntry(laddr);
    DirState state = directory.GetState(entry);
    if (directory.InTransitionState(state)) {
      epicLog(LOG_INFO, "directory in transition state when local write %d",
              state);
      AddToServeLocalRequest(start_blk, wr);
      directory.unlock(laddr);
      wr->unlock();
      return IN_TRANSITION;
    }
    if (DIR_DIRTY == state || DIR_SHARED == state) {
      list<GAddr>& shared = directory.GetSList(entry);
      WorkRequest* lwr = new WorkRequest(*wr);
      lwr->counter = 0;
      lwr->op = state == DIR_DIRTY ? FETCH_AND_INVALIDATE : INVALIDATE;
      lwr->addr = start_blk;
      lwr->size = BLOCK_SIZE;
      lwr->ptr = laddr;
      lwr->parent = wr;
      lwr->flag |= LOCKED;
      lwr->id = GetWorkPsn();
      lwr->counter = shared.size();
      wr->counter++;
      epicAssert(state != DIR_TO_UNSHARED);
      epicAssert(
          (state == DIR_DIRTY && !directory.IsBlockLocked(entry))
              || (state == DIR_SHARED && !directory.IsBlockWLocked(entry)));
      directory.ToToUnShared(entry);
      AddToPending(lwr->id, lwr);
      for (auto it = shared.begin(); it != shared.end(); it++) {
        Client* cli = GetClient(*it);
        epicLog(
            LOG_DEBUG,
            "invalidate (%d) cache from worker %d, state = %d, lwr->counter = %d",
            lwr->op, cli->GetWorkerId(), state, lwr->counter.load());
        SubmitRequest(cli, lwr);
        //lwr->counter++;
      }
    } else if (DIR_UNSHARED == state) {
      int ret;
      if (entry) {
        ret = directory.WLock(entry, ToLocal(wr->addr));
      } else {
        ret = directory.WLock(ToLocal(wr->addr));
      }
      if (ret) {  //failed to lock
        epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
        if (wr->flag & TRY_LOCK) {
          wr->status = LOCK_FAILED;
        } else {
          AddToServeLocalRequest(start_blk, wr);
          directory.unlock(laddr);
          wr->unlock();
          return IN_TRANSITION;
        }
      }
    }
    if (wr->counter) {
      directory.unlock(laddr);
      wr->unlock();
      return REMOTE_REQUEST;
    } else {
      directory.unlock(laddr);
      wr->unlock();
    }
  } else {
    int ret = cache.WLock(wr);
    if (ret) {
      return REMOTE_REQUEST;
    }
  }
#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  } else {
    epicAssert(wr->is_cache_hit_);
    if (IsLocal(wr->addr)) {
        ++no_local_writes_;
        ++no_local_writes_hit_;
    } else {
        ++no_remote_writes_;
        ++no_remote_writes_hit_;
    }
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalUnLock(WorkRequest* wr) {
  if (!(wr->flag & FENCE)) {
    Fence* fence = fences_.at(wr->fd);
    fence->lock();
    if (IsFenced(fence, wr)) {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    } else if (fence->pending_writes) {  //we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from UNLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }

  if (IsLocal(wr->addr)) {
    GAddr start_blk = TOBLOCK(wr->addr);
    void* laddr = ToLocal(start_blk);
    directory.lock(laddr);
    directory.UnLock(ToLocal(wr->addr));
    directory.unlock(laddr);
  } else {
    cache.UnLock(wr->addr);
  }
  ProcessToServeRequest(wr);
#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr)) {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}
#endif //if not NOCACHE

int Worker::ProcessLocalRequest(WorkRequest* wr) {
  epicLog(
      LOG_DEBUG,
      "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
      wr->op, wr->flag, wr->addr, wr->size, wr->fd);
  int ret = SUCCESS;
  if (MALLOC == wr->op) {
    ret = ProcessLocalMalloc(wr);
  } else if (FREE == wr->op) {
    ret = ProcessLocalFree(wr);
  } else if (READ == wr->op) {
    ret = ProcessLocalRead(wr);
  } else if (WRITE == wr->op) {
    ret = ProcessLocalWrite(wr);
  } else if (MFENCE == wr->op) {
    ret = ProcessLocalMFence(wr);
  } else if (SFENCE == wr->op) {
    ret = ProcessLocalSFence(wr);
  } else if (RLOCK == wr->op) {  //fence for every lock
    ret = ProcessLocalRLock(wr);
  } else if (WLOCK == wr->op) {
    ret = ProcessLocalWLock(wr);
  } else if (UNLOCK == wr->op) {
    ret = ProcessLocalUnLock(wr);
  } else if (GET == wr->op) {
    SubmitRequest(master, wr, ADD_TO_PENDING | REQUEST_SEND);
    ret = REMOTE_REQUEST;
  } else if (PUT == wr->op) {
    SubmitRequest(master, wr);
#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
      /*
       * notify the app thread directly
       * this can only happen when the request can be fulfilled locally
       * or we don't need to wait for reply from remote node
       */
      if (Notify(wr)) {
        epicLog(LOG_WARNING, "cannot wake up the app thread");
      }
#ifdef MULTITHREAD
    }
#endif
    ret = SUCCESS;

#ifdef DHT
  } else if (GET_HTABLE == wr->op) {
    ret = this->ProcessLocalHTable(wr);
#endif

  } else {
    wr->status = UNRECOGNIZED_OP;
    epicLog(LOG_WARNING, "unrecognized op %d from local thread %d", wr->op,
            wr->fd);
    exit(-1);
  }
  return ret;
}

void Worker::ProcessLocalRequest(aeEventLoop *el, int fd, void *data,
                                 int mask) {
  char buf[1];
  if (1 != read(fd, buf, 1)) {
    epicLog(LOG_WARNING, "read pipe failed (%d:%s)", errno, strerror(errno));
  }
  epicLog(LOG_DEBUG, "receive local request %c", buf[0]);

  Worker* w = (Worker*) data;
  WorkRequest* wr;
  int i = 0;
  while (w->wqueue->pop(wr)) {
    i++;
    epicLog(
        LOG_DEBUG,
        "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
        wr->op, wr->flag, wr->addr, wr->size, wr->fd);
    w->ProcessLocalRequest(wr);
  }
  if (!i)
    epicLog(LOG_DEBUG, "pop %d from work queue", i);
}

