// Copyright (c) 2018 The GAM Authors 

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

