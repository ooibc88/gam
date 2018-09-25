// Copyright (c) 2018 The GAM Authors 

void Worker::ProcessRemoteRead(Client* client, WorkRequest* wr) {
  epicAssert(IsLocal(wr->addr));
  GAddr start = wr->addr;
  GAddr start_blk = TOBLOCK(start);
  GAddr end = GADD(start, wr->size);
  if(TOBLOCK(end-1) != start_blk) {
    epicLog(LOG_INFO, "read/write split to multiple blocks");
  }
  epicAssert(!(wr->flag & COPY) || ((wr->flag & COPY) && (wr->flag & ASYNC)));

  char buf[wr->size];
  for(GAddr i = start_blk; i < end;) {
    GAddr nextb = BADD(i, 1);
    void* laddr = ToLocal(i);
    directory.lock(laddr);
    GAddr gs = i > start ? i : start;
    void* ls = (void*)((ptr_t)buf + GMINUS(gs, start));
    int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
    memcpy(ls, ToLocal(gs), len);
    directory.unlock(laddr);
    i = nextb;
  }

  wr->ptr = buf;
  wr->op = READ_REPLY;
  SubmitRequest(client, wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteReadCache(Client* client, WorkRequest* wr) {
  epicLog(LOG_WARNING, "Impossible!");
}

void Worker::ProcessRemoteReadReply(Client* client, WorkRequest* wr) {
  epicAssert(!wr->status);
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  epicAssert(pwr->id == wr->id);
  epicAssert(pwr->size == wr->size);
  memcpy(pwr->ptr, wr->ptr, wr->size);
  pwr->status = wr->status;
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  Notify(pwr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteWrite(Client* client, WorkRequest* wr) {
  Work op_orin = wr->op;
  epicAssert(IsLocal(wr->addr));  //I'm the home node
  GAddr start = wr->addr;
  GAddr start_blk = TOBLOCK(start);
  GAddr end = GADD(start, wr->size);
  for(GAddr i = start_blk; i < end;) {
    GAddr nextb = BADD(i, 1);
    void* laddr = ToLocal(i);
    directory.lock(laddr);
    GAddr gs = i > start ? i : start;
    void* ls = (void*)((ptr_t)wr->ptr + GMINUS(gs, start));
    int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
    memcpy(ToLocal(gs), ls, len);
    directory.unlock(laddr);
    i = nextb;
  }
  wr->op = WRITE_REPLY;
  wr->status = SUCCESS;
  SubmitRequest(client, wr);  //in order to support fence
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteWriteCache(Client* client, WorkRequest* wr) {
  epicLog(LOG_WARNING, "Impossible!");
}

void Worker::ProcessRemoteWriteReply(Client* client, WorkRequest* wr) {
  epicAssert(!wr->status);
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  epicAssert(pwr->id == wr->id);
  pwr->status = wr->status;
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  Notify(pwr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteRLock(Client* client, WorkRequest* wr) {
  epicAssert(wr->size == 0 && wr->ptr == nullptr);
  epicAssert(IsLocal(wr->addr));
  GAddr start = wr->addr;
  GAddr start_blk = TOBLOCK(start);
  void* laddr = ToLocal(start_blk);

  directory.lock(laddr);
  int ret = directory.RLock(ToLocal(wr->addr));
  if(ret) {  //failed to lock
    epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
    if(wr->flag & TRY_LOCK) {
      wr->status = LOCK_FAILED;
    } else {
      //to_serve_local_requests[start_blk].push(wr);
      AddToServeRemoteRequest(start_blk, client, wr);
      directory.unlock(laddr);
      return;
    }
  } else {
    wr->status = SUCCESS;
  }
  directory.unlock(laddr);
  wr->op = RLOCK_REPLY;
  SubmitRequest(client, wr);  //no need for reply
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteWLock(Client* client, WorkRequest* wr) {
  epicAssert(wr->size == 0 && wr->ptr == nullptr);
  epicAssert(IsLocal(wr->addr));
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
      AddToServeRemoteRequest(start_blk, client, wr);
      directory.unlock(laddr);
      return;
    }
  }
  directory.unlock(laddr);
  wr->op = WLOCK_REPLY;
  SubmitRequest(client, wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteUnLock(Client* client, WorkRequest* wr) {
  epicAssert(wr->size == 0 && wr->ptr == nullptr);
  epicAssert(IsLocal(wr->addr));
  GAddr start_blk = TOBLOCK(wr->addr);
  void* laddr = ToLocal(start_blk);
  //epicAssert(!directory.InTransitionState(laddr));
  directory.lock(laddr);
  directory.UnLock(ToLocal(wr->addr));
  directory.unlock(laddr);
  ProcessToServeRequest(wr);
#ifndef ASYNC_UNLOCK
  wr->op = UNLOCK_REPLY;
  wr->status = SUCCESS;
  SubmitRequest(client, wr);
#endif
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteUnLockReply(Client* client, WorkRequest* wr) {
#ifdef ASYNC_UNLOCK
  epicAssert(false);
#endif
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  epicAssert(pwr->id == wr->id);
  pwr->status = wr->status;
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  epicAssert(!pwr->status);
  Notify(pwr);
  delete wr;
  wr= nullptr;
}

void Worker::ProcessRemoteLockReply(Client* client, WorkRequest* wr) {
  epicAssert(wr->size == 0 && wr->ptr == nullptr);
  WorkRequest* pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  epicAssert(pwr->id == wr->id);
  pwr->status = wr->status;
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  if(pwr->status) {
    epicLog(LOG_INFO, "lock remote addr %lx failed", wr->addr);
  }
  Notify(pwr);
  delete wr;
  wr= nullptr;
}

