// Copyright (c) 2018 The GAM Authors
#include "worker.h"

void Worker::LearnWriteWithImm(Client *client, WorkRequest *wr)
{
}

void Worker::ProcessRemoteRead(Client *client, WorkRequest *wr)
{
  // Just_for_test("remote read", wr);
  epicAssert(IsLocal(wr->addr));
#ifdef SELECTIVE_CACHING
  void *laddr = ToLocal(TOBLOCK(wr->addr));
#else
  void *laddr = ToLocal(wr->addr);
#endif
  directory.lock(laddr);
  DirEntry *entry = directory.GetEntry(laddr);
#ifdef SUB_BLOCK
  if (wr->flag & Write_shared) {
    entry = directory.GetSubEntry(laddr);
#ifdef DYNAMIC
    //判断版本是否一致
    if (wr->Version != directory.GetVersion(entry) ) { //版本不一致，需要重新发
      /*
        由于已经原子性的使得当前目录为unshared状态后，再做状态转换
        所以这里版本仍然不一致，只能是副本节点cache处于invalid情况下，发送读请求（变为Cache_to_shared)
        所以对于读操作可以用write_reply打回去之后，重新发送两个新的请求过来？
        或许可以简单粗暴的加入toserverequest再执行？
      */
      wr->status = 5732;
      wr->op = WRITE_REPLY;
      SubmitRequest(client, wr);
      directory.unlock(laddr);
      return;
    }
#endif
  }
#endif
  if (directory.InTransitionState(entry))
  {
    // to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in Transition State %d",
            directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  if (directory.GetState(entry) != DIR_DIRTY)
  { // it is shared or exclusively owned (Case 2)
    // add the lock support
    if (directory.IsBlockWLocked(entry))
    {
      if (wr->flag & TRY_LOCK)
      { // reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = READ_REPLY;
        directory.unlock(laddr);
        SubmitRequest(client, wr);
        delete wr;
        wr = nullptr;
      }
      else
      {
        // to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
        AddToServeRemoteRequest(wr->addr, client, wr);
        directory.unlock(laddr);
      }
      epicLog(LOG_INFO, "addr %lx is exclusively locked by %d", ToGlobal(laddr),
              GetWorkerId());
      return;
    }

    // TODO: add the write completion check
    epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
    // client: the node that sends the request
    // wr->ptr: the address of the local node
    client->WriteWithImm(wr->ptr, ToLocal(wr->addr), wr->size, wr->id);
#ifdef SELECTIVE_CACHING
    if (!(wr->flag & NOT_CACHE))
    {
#endif
      if (entry)
      {
        epicAssert(
            directory.GetState(entry) == DIR_UNSHARED || directory.GetState(entry) == DIR_SHARED);
        directory.ToShared(entry, client->ToGlobal(wr->ptr));
      }
      else
      {
        epicAssert(directory.GetState(entry) == DIR_UNSHARED);
        directory.ToShared(laddr, client->ToGlobal(wr->ptr));
      }
#ifdef SELECTIVE_CACHING
    }
#endif
    delete wr;
    wr = nullptr;
  }
  else
  {
    epicAssert(!directory.IsBlockLocked(entry));
    WorkRequest *lwr = new WorkRequest(*wr);
    lwr->counter = 0;
    lwr->op = READ_FORWARD;
    lwr->parent = wr;
    lwr->pid = wr->id;
    lwr->pwid = client->GetWorkerId();

    GAddr rc = directory.GetSList(entry).front(); // only one worker is updating this line
    Client *cli = GetClient(rc);
#ifdef SELECTIVE_CACHING
    if (!(wr->flag & NOT_CACHE))
    {
      // intermediate state
      directory.ToToShared(entry);
      SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
    }
    else
    {
      SubmitRequest(cli, lwr);
    }
#else
    // intermediate state
    directory.ToToShared(entry);
    SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
#endif
  }
  directory.unlock(laddr);
}

void Worker::ProcessRemoteReadCache(Client *client, WorkRequest *wr)
{
  Work op_orin = wr->op;
  bool deadlock = false;
#ifndef SELECTIVE_CACHING
  epicAssert(BLOCK_ALIGNED(wr->addr));
#endif
  GAddr blk = TOBLOCK(wr->addr);
#ifdef SUB_BLOCK
  blk = wr->addr;
#endif
  cache.lock(blk);
  CacheLine *cline = cache.GetCLine(blk);
  if (!cline)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find an updated copy");
    wr->op = READ_REPLY; // change op to the corresponding reply type
    wr->status = READ_ERROR;
    if (FETCH_AND_SHARED == op_orin)
    {
      SubmitRequest(client, wr);
    }
    else
    {                            // READ_FORWARD
      SubmitRequest(client, wr); // reply to the home node
      Client *cli = FindClientWid(wr->pwid);
      wr->id = wr->pid;
      SubmitRequest(cli, wr); // reply to the local node
    }
  }
  else
  {
    if (cache.InTransitionState(cline->state))
    {
      if (cline->state == CACHE_TO_DIRTY)
      {
        // to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
        AddToServeRemoteRequest(wr->addr, client, wr);
        epicLog(LOG_INFO, "cache in transition state %d", cline->state);
        cache.unlock(blk);
        return;
      }
      else
      {
        // deadlock: this node wants to give up the ownership
        // meanwhile, another node wants to read
        epicLog(LOG_INFO, "!!!deadlock detected!!!\n");
        epicAssert(cline->state == CACHE_TO_INVALID);
        deadlock = true;
      }
    }

    // add the lock support
    if (cache.IsBlockWLocked(cline))
    {
      if (wr->flag & TRY_LOCK)
      { // reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = READ_REPLY;
        if (FETCH_AND_SHARED == op_orin)
        {
          SubmitRequest(client, wr);
        }
        else
        {                            // READ_FORWARD
          SubmitRequest(client, wr); // reply to the home node
          Client *cli = FindClientWid(wr->pwid);
          wr->id = wr->pid;
          SubmitRequest(cli, wr); // reply to the local node
        }
        delete wr;
        wr = nullptr;
        cache.unlock(blk);
      }
      else
      {
        epicAssert(!deadlock);
        // we must unlock the cache/directory lock before calling the AddToServe[Remote]Request
        // as the lock acquire seq is fences -> directory/cache -> to_serve_local/remote_request/pending_works
        // the ProcessToServeRequest() breaks this rule
        // we copy the queue first and then release the to_serve.._request lock immediately
        // to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
        AddToServeRemoteRequest(wr->addr, client, wr);
        cache.unlock(blk);
      }
      epicLog(LOG_INFO, "addr %lx is exclusively locked by %d", blk,
              GetWorkerId());
      return;
    }

    // TODO: add the write completion check
    // can add it to the pending work and check it upon done
    if (op_orin == FETCH_AND_SHARED)
    {
#ifdef SELECTIVE_CACHING
      epicAssert(wr->size == BLOCK_SIZE && wr->addr == blk);
#endif
      epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
      client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id); // reply to the local home node
    }
    else
    { // READ_FORWARD
      Client *cli = FindClientWid(wr->pwid);

#ifdef SELECTIVE_CACHING
      void *cs = (void *)((ptr_t)cline->line + GMINUS(wr->addr, blk));
      if (!(wr->flag & NOT_CACHE))
      {
        epicAssert(wr->size == BLOCK_SIZE && wr->addr == blk && cs == cline->line);
      }
      epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
      cli->WriteWithImm(wr->ptr, cs, wr->size, wr->pid); // reply to the local node
      if (!(wr->flag & NOT_CACHE))
      {
        epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
        client->WriteWithImm(client->ToLocal(blk), cline->line, BLOCK_SIZE, wr->id); // writeback to home node
      }
#else
      epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
      cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid); // reply to the local node
#ifdef SUB_BLOCK
      if (wr->flag & Write_shared) {
        client->WriteWithImm(client->ToLocal(blk), cline->line, cline->CacheSize,
          wr->id);  //writeback to home node
      }
      else {
        client->WriteWithImm(client->ToLocal(blk), cline->line, BLOCK_SIZE,
          wr->id);  //writeback to home node
      }
#else
      client->WriteWithImm(client->ToLocal(blk), cline->line, BLOCK_SIZE,
          wr->id);  //writeback to home node
#endif
#endif
    }

#ifdef SELECTIVE_CACHING
    if (!(wr->flag & NOT_CACHE))
    {
#endif
      // TOOD: add below to the callback function
      if (!deadlock)
        cache.ToShared(cline);
#ifdef SELECTIVE_CACHING
    }
#endif
  }
  cache.unlock(blk);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteReadReply(Client *client, WorkRequest *wr)
{
  /*
   * READ/RLock failed case,
   * as normal successful case is through write_with_imm
   * which will call ProcessRequest(Client*, unsigned int)
   */
  epicAssert(READ_ERROR == wr->status || LOCK_FAILED == wr->status);
  WorkRequest *pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  epicAssert(pwr->id == wr->id);
  epicAssert((pwr->flag & LOCKED) && (pwr->flag & TRY_LOCK));
  switch (pwr->op)
  {
  case READ:             // local node
  case FETCH_AND_SHARED: // local and home node
  {
    // For read, it's ok to undo the change and clear pending work now
    // as there should be only one responder
    epicAssert(pwr->parent);
    pwr->parent->lock();

    // undo the directory/cache changes
    if (READ == pwr->op)
    {
      epicAssert(pwr->flag & CACHED);
      cache.lock(pwr->addr);
      cache.ToInvalid(pwr->addr);
      cache.unlock(pwr->addr);
    }
    else
    { // FETCH_AND_SHARED
      epicAssert(IsLocal(pwr->addr));
      epicAssert(pwr->ptr == ToLocal(pwr->addr));
      directory.lock(pwr->ptr);
      directory.UndoDirty(pwr->ptr);
      directory.unlock(pwr->ptr);
    }

    pwr->parent->status = wr->status;
    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
    pwr->parent->unlock();
    Notify(pwr->parent);
    break;
  }
  case READ_FORWARD: // home node
  {
    epicAssert(pwr->parent);
    epicAssert(IsLocal(pwr->addr)); // I'm the home node
    // parent request is from local node
    WorkRequest *parent = pwr->parent;
    void *laddr = ToLocal(pwr->addr);

    // For read, it's ok to undo the change and clear pending work now
    // as there should be only one responder
    directory.lock(laddr);
    directory.UndoDirty(laddr);
    directory.unlock(laddr);
    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
    delete parent;
    parent = nullptr;
    break;
  }
  default:
    epicLog(LOG_WARNING,
            "Unrecognized pending work request %d for WRITE_REPLY", pwr->op);
    exit(-1);
    break;
  }

  ProcessToServeRequest(pwr);
  delete pwr;
  delete wr;
  pwr = nullptr;
  wr = nullptr;
}

void Worker::ProcessRemoteWrite(Client *client, WorkRequest *wr)
{
  Work op_orin = wr->op;
#ifndef SELECTIVE_CACHING
  epicAssert(wr->size == BLOCK_SIZE);
  epicAssert(BLOCK_ALIGNED(wr->addr));
#endif
  epicAssert(IsLocal(wr->addr)); // I'm the home node
#ifdef SELECTIVE_CACHING
  void *laddr = ToLocal(TOBLOCK(wr->addr));
#else
  void *laddr = ToLocal(wr->addr);
#endif
  epicAssert(BLOCK_ALIGNED((uint64_t)laddr));
  directory.lock(laddr);
  DirEntry* entry;
#ifdef SUB_BLOCK
  if (wr->flag & Write_shared) {
    //epicLog(LOG_WARNING, "remote sub write here");
    entry = directory.GetSubEntry(laddr);
#ifdef DYNAMIC
    //判断版本是否一致
    if (wr->Version != directory.GetVersion(entry) ) { //版本不一致，需要重新发
      wr->status = 5732;
      wr->op = WRITE_REPLY;
      SubmitRequest(client, wr);
      directory.unlock(laddr);
      return;
    }
#endif
  }
  else entry = directory.GetEntry(laddr);
#else
  entry = directory.GetEntry(laddr);
#endif

  DirState state = directory.GetState(entry);
  if (directory.InTransitionState(state))
  {
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "Directory in Transition State %d", state);
    directory.unlock(laddr);
    return;
  }
  if (state != DIR_DIRTY)
  {
    // add the lock support
    if (directory.IsBlockLocked(entry))
    {
      epicAssert((directory.IsBlockWLocked(entry) && state == DIR_UNSHARED) || !directory.IsBlockWLocked(entry));
      if (wr->flag & TRY_LOCK)
      { // reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = WRITE_REPLY;
        wr->counter = 0;
        SubmitRequest(client, wr);
        delete wr;
        wr = nullptr;
        directory.unlock(laddr);
      }
      else
      {
        AddToServeRemoteRequest(wr->addr, client, wr);
        directory.unlock(laddr);
      }
      epicLog(LOG_INFO, "addr %lx is locked by %d", ToGlobal(laddr),
              GetWorkerId());
      return;
    }

    if (state == DIR_SHARED)
    {
      // change the invalidate strategy (home node accepts invalidation responses)
      // in order to simply the try_lock failed case
      list<GAddr> &shared = directory.GetSList(entry);
      WorkRequest *lwr = new WorkRequest(*wr);
#ifdef SELECTIVE_CACHING
      if (wr->flag & NOT_CACHE)
      {
        epicAssert(wr->size <= BLOCK_SIZE);
        lwr->addr = TOBLOCK(wr->addr);
        lwr->size = BLOCK_SIZE;
        lwr->ptr = (void *)((ptr_t)lwr->ptr - GMINUS(wr->addr, lwr->addr)); // not necessary
      }
#endif

      lwr->lock();
      lwr->counter = 0;
      lwr->op = INVALIDATE_FORWARD;
      lwr->parent = wr;
      lwr->id = GetWorkPsn();
      lwr->pwid = client->GetWorkerId();
      lwr->counter = shared.size();
      bool first = true;
      for (auto it = shared.begin(); it != shared.end(); it++)
      {
        Client *cli = GetClient(*it);
        if (cli == client)
        {
          epicAssert(op_orin == WRITE_PERMISSION_ONLY);
          lwr->counter--;
          continue;
        }
        epicLog(LOG_DEBUG, "invalidate forward (%d) cache from worker %d",
                lwr->op, cli->GetWorkerId());
        if (first)
        {
          AddToPending(lwr->id, lwr);
          first = false;
        }
        SubmitRequest(cli, lwr);
        // lwr->counter++;
      }

      if (lwr->counter)
      {
        lwr->unlock();
        directory.ToToDirty(entry);
        directory.unlock(laddr);
        return; // return and wait for reply
      }
      else
      {
        lwr->unlock();
        epicAssert(op_orin == WRITE_PERMISSION_ONLY);
        delete lwr;
        lwr = nullptr;
      }
    }
    else
    { // DIR_UNSHARED
#ifdef SELECTIVE_CACHING
      if (wr->flag & NOT_CACHE)
      {
#ifdef GFUNC_SUPPORT
        if (wr->flag & GFUNC)
        {
          epicAssert(wr->gfunc);
          epicAssert(TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size - 1)));
          void *laddr = ToLocal(wr->addr);
          wr->gfunc(laddr, wr->arg);
        }
        else
        {
#endif
          memcpy(ToLocal(wr->addr), wr->ptr, wr->size);
#ifdef GFUNC_SUPPORT
        }
#endif
      }
      else
      {
#endif
        if (WRITE == op_orin)
        {
          epicLog(LOG_DEBUG, "write the data (size = %ld) to destination",
                  wr->size);
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->Write(wr->ptr, laddr, wr->size);
        }
        else
        { // WRITE_PERMISSION_ONLY
          epicAssert(state == DIR_UNSHARED);
          // deadlock: one node (Node A) wants to update its cache from shared to dirty,
          // but at the same time, the home nodes invalidates all its shared copy (due to a local write)
          // currently, dir_state == dir_unshared (after pend the request because it was dir_to_unshared)
          // solution: Node A acts as it is still a shared copy so that the invalidation can completes,
          // after which, home node processes the pending list and change the WRITE_PERMISSION_ONLY to WRITE
          epicLog(LOG_DEBUG, "write the data to destination");
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->Write(wr->ptr, laddr, wr->size);
          epicLog(LOG_INFO, "deadlock detected");
        }
#ifdef SELECTIVE_CACHING
      }
#endif
    }
    epicAssert(!directory.InTransitionState(entry));
    wr->op = WRITE_REPLY;
    wr->status = SUCCESS;
    wr->counter = 0;
    SubmitRequest(client, wr);

#ifdef SELECTIVE_CACHING
    if (!(wr->flag & NOT_CACHE))
    {
#endif

      // we can safely change the directory as we've already transfered the data to the local node
      //  logging
      logOwner(client->GetWorkerId(), wr->addr);
      if (entry)
      {
        directory.ToDirty(entry, client->ToGlobal(wr->ptr));
      }
      else
      {
        directory.ToDirty(laddr, client->ToGlobal(wr->ptr)); // entry is null
      }

#ifdef SELECTIVE_CACHING
    }
#endif
    delete wr;
    wr = nullptr;
  }
  else
  { // Case 4
    epicAssert(!directory.IsBlockLocked(entry));
    WorkRequest *lwr = new WorkRequest(*wr);
#ifdef SELECTIVE_CACHING
    if (wr->flag & NOT_CACHE)
    {
      epicAssert(wr->size <= BLOCK_SIZE);
      lwr->addr = TOBLOCK(wr->addr);
      lwr->size = BLOCK_SIZE;
      lwr->ptr = (void *)((ptr_t)lwr->ptr - GMINUS(wr->addr, lwr->addr)); // not necessary
    }
#endif
    lwr->counter = 0;
    if (WRITE == op_orin || WLOCK == op_orin)
    {
      lwr->op = WRITE_FORWARD;
    }
    else if (WRITE_PERMISSION_ONLY == op_orin)
    {
      // deadlock: WRITE_PERMISSION_ONLY shouldn't co-exist with DIR_DIRTY state
      // there must be a race where one nodes (Node A) tries to update its cache from shared to dirty,
      // while another node (Node B) writes the data before that node
      // solution: Node A replies as its cache line is shared, and home node changes it to WRITE_FORWARD
      // lwr->op = WRITE_PERMISSION_ONLY_FORWARD;
      lwr->op = WRITE_FORWARD;
    }
    lwr->parent = wr;
    lwr->pid = wr->id;
    lwr->pwid = client->GetWorkerId();

    GAddr rc = directory.GetSList(entry).front(); // only one worker is updating this line
    Client *cli = GetClient(rc);

    /* add xmx add */
    if (op_orin == WRITE) {
      racetime += 1;
#ifdef DYNAMIC
      entry->Race_time += 1;
#endif
    }
    /* add xmx add */

    // intermediate state
    directory.ToToDirty(entry);
    SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
  }
  directory.unlock(laddr);
}

void Worker::ProcessRemoteWriteCache(Client *client, WorkRequest *wr)
{
  epicAssert(wr->op != WRITE_PERMISSION_ONLY_FORWARD); // this cannot happen
  Work op_orin = wr->op;
  bool deadlock = false;
  epicAssert(wr->size == BLOCK_SIZE);
  epicAssert(BLOCK_ALIGNED(wr->addr));
  epicAssert(!IsLocal(wr->addr)); // I'm not the home node
  // we hold an updated copy of the line (WRITE_FORWARD: Case 4)
  GAddr to_lock = wr->addr;
  cache.lock(to_lock);
  CacheLine* cline;
#ifdef SUB_BLOCK
  if (wr->flag & Write_shared) {
    //epicLog(LOG_WARNING, "invalid forward cache");
    cline = cache.GetSubCline(wr->addr);
  }
  else cline = cache.GetCLine(wr->addr);
#else
  cline = cache.GetCLine(wr->addr);
#endif
  if (!cline)
  {
    if (INVALIDATE == op_orin || INVALIDATE_FORWARD == op_orin)
    {
      // this should because of cache line eviction from shared to invalid
      // so we reply as if it is shared
      deadlock = true;

      // TODO: add the write completion check
      // can add it to the pending work and check it upon done
      if (wr->op == INVALIDATE)
      { // INVALIDATE
        client->WriteWithImm(nullptr, nullptr, 0, wr->id);
      }
      else
      { // INVALIDATE_FORWARD
        //			Client* cli = FindClientWid(wr->pwid);
        //			cli->WriteWithImm(nullptr, nullptr, 0, wr->pid); //reply the new owner
        //			epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
        //      after change the invalidate_forward strategy
        client->WriteWithImm(nullptr, nullptr, 0, wr->id);
        epicLog(LOG_DEBUG, "send to %d with id %d", client->GetWorkerId(),
                wr->id);
      }
    }
    else
    {
      epicLog(LOG_FATAL, "Unexpected: cannot find an updated copy");
      wr->op = WRITE_REPLY; // change op to the corresponding reply type
      wr->status = WRITE_ERROR;
      if (INVALIDATE == op_orin || FETCH_AND_INVALIDATE == op_orin)
      {
        SubmitRequest(client, wr);
      }
      else if (INVALIDATE_FORWARD == op_orin)
      {
        //			Client* cli = FindClientWid(wr->pwid);
        //			wr->id = wr->pid;
        //			SubmitRequest(cli, wr);
        SubmitRequest(client, wr);
      }
      else
      { // WRITE_FORWARD or WRITE_PERMISSION_ONLY_FORWARD
        SubmitRequest(client, wr);
        Client *cli = FindClientWid(wr->pwid);
        wr->id = wr->pid;
        SubmitRequest(cli, wr);
      }
    }
    delete wr;
    wr = nullptr;
  }
  else
  {
    if (cache.InTransitionState(cline->state))
    {
      /*
       * deadlock, since the responding node must just change its cache state
       * and send request to home node,
       * who was not notified of the change and sent an invalidate/forward request.
       * How to solve?
       * there are two causes: cache from shared to dirty (ToDirty State)
       * cache from dirty to invalid (ToInvalid state)
       */
      if ((INVALIDATE == wr->op || INVALIDATE_FORWARD == wr->op) && cline->state == CACHE_TO_DIRTY)
      {
        // deadlock case 1
        epicLog(LOG_INFO, "!!!deadlock detected!!!");
        deadlock = true;
      }
      else
      {
        if (cline->state == CACHE_TO_INVALID)
        {
          // deadlock case 2
          epicLog(LOG_INFO, "!!!deadlock detected!!!");
          deadlock = true;
        }
        else
        {
          AddToServeRemoteRequest(wr->addr, client, wr);
          epicLog(LOG_INFO, "cache in transition state %d", cline->state);
          cache.unlock(to_lock);
          return;
        }
      }
    }

    // add the lock support
    if (cache.IsBlockLocked(cline))
    {
      if (wr->flag & TRY_LOCK)
      { // reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = WRITE_REPLY;
        if (INVALIDATE == op_orin || FETCH_AND_INVALIDATE == op_orin)
        {
          SubmitRequest(client, wr);
        }
        else if (INVALIDATE_FORWARD == op_orin)
        {
          //				Client* cli = FindClientWid(wr->pwid);
          //				wr->id = wr->pid;
          //				SubmitRequest(cli, wr);
          SubmitRequest(client, wr);
        }
        else
        { // WRITE_FORWARD or WRITE_PERMISSION_ONLY_FORWARD
          SubmitRequest(client, wr);
          Client *cli = FindClientWid(wr->pwid);
          wr->id = wr->pid;
          SubmitRequest(cli, wr);
        }
        cache.unlock(to_lock);
        delete wr;
        wr = nullptr;
        return;
      }
      else
      {
        // deadlock case 3
        // if it is rlocked, and in deadlock status (in transition state from shared to dirty)
        // we are still safe to act as it was in shared state and ack the invalidation request
        // because the intransition state will block other r/w requests
        // until we get replies from the home node (then WRITE_PERMISSION_ONLY has
        // been changed to WRITE by the home node as agreed)
        if (!deadlock)
        {
          AddToServeRemoteRequest(wr->addr, client, wr);
          epicLog(LOG_INFO, "addr %lx is locked by %d", wr->addr,
                  GetWorkerId());
          cache.unlock(to_lock);
          return;
        }
        else
        {
          epicLog(LOG_WARNING, "Deadlock detected");
        }
      }
    }

    // TODO: add the write completion check
    // can add it to the pending work and check it upon done
    if (wr->op == FETCH_AND_INVALIDATE)
    { // FETCH_AND_INVALIDATE
      epicAssert(cache.IsDirty(cline) || cache.InTransitionState(cline));
      if (deadlock)
      {
        epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
        client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);
        delete wr;
        wr = nullptr;
      }
      else
      {
        //			client->WriteWithImm(wr->ptr, line, wr->size, wr->id);
        //			cache.ToInvalid(wr->addr);
        //			delete wr;
        unsigned int orig_id = wr->id;
        wr->status = deadlock;
        wr->id = GetWorkPsn();
        wr->op = PENDING_INVALIDATE;
        AddToPending(wr->id, wr);
        cache.ToToInvalid(cline);
        epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
        client->WriteWithImm(wr->ptr, cline->line, wr->size, orig_id, wr->id,
                             true);
      }
    }
    else if (wr->op == INVALIDATE)
    { // INVALIDATE
      epicAssert(!cache.IsDirty(cline));
      client->WriteWithImm(nullptr, nullptr, 0, wr->id);
      // TOOD: add below to the callback function
      if (!deadlock)
        cache.ToInvalid(cline);
      delete wr;
      wr = nullptr;
    }
    else if (wr->op == INVALIDATE_FORWARD)
    { // INVALIDATE_FORWARD
      epicAssert(!cache.IsDirty(cline));
      //		Client* cli = FindClientWid(wr->pwid);
      //		cli->WriteWithImm(nullptr, nullptr, 0, wr->pid); //reply the new owner
      //		epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
      client->WriteWithImm(nullptr, nullptr, 0, wr->id);
      epicLog(LOG_DEBUG, "send to %d with id %d", client->GetWorkerId(),
              wr->id);
      if (!deadlock)
        cache.ToInvalid(cline);
      delete wr;
      wr = nullptr;
    }
    else
    { // WRITE_FORWARD
      Client *cli = FindClientWid(wr->pwid);
      if (deadlock)
      {
#ifdef SELECTIVE_CACHING
        if (wr->flag & NOT_CACHE)
        {
          // client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);  //transfer ownership
          // fix bug here (wr->ptr is not the same as ToLocal(wr->addr)
          // and here we write the dirty data back to the home node rather than
          // the local node requesting the data
          epicAssert(BLOCK_ALIGNED(wr->addr));
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->WriteWithImm(client->ToLocal(wr->addr), cline->line, wr->size, wr->id); // transfer ownership
        }
        else
        {
#endif
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid); // reply the new owner
          epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
          client->WriteWithImm(nullptr, nullptr, 0, wr->id); // transfer ownership
#ifdef SELECTIVE_CACHING
        }
#endif
        delete wr;
        wr = nullptr;
      }
      else
      {
        //		  cli->WriteWithImm(wr->ptr, line, wr->size, wr->pid); //reply the new owner
        //		  epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
        //			client->WriteWithImm(nullptr, nullptr, 0, wr->id); //transfer ownership
        //			cache.ToInvalid(wr->addr);
        //			delete wr;
        unsigned int orig_id = wr->id;
        epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
        wr->id = GetWorkPsn();
        wr->op = PENDING_INVALIDATE;
        AddToPending(wr->id, wr);
        cache.ToToInvalid(cline);
#ifdef SELECTIVE_CACHING
        if (wr->flag & NOT_CACHE)
        {
          epicAssert(BLOCK_ALIGNED(wr->addr));
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->WriteWithImm(client->ToLocal(wr->addr), cline->line, wr->size, orig_id, wr->id, true); // transfer ownership
        }
        else
        {
#endif
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid, wr->id, true); // reply the new owner
          client->WriteWithImm(nullptr, nullptr, 0, orig_id);                       // transfer ownership
#ifdef SELECTIVE_CACHING
        }
#endif
      }
    }
  }
  cache.unlock(to_lock);
}

void Worker::ProcessRemoteWriteReply(Client *client, WorkRequest *wr)
{
  WorkRequest *pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  epicAssert(pwr->id == wr->id);
  if (wr->status)
  {
    // don't need to lock parent
    // backup these data to make sure it is valid even delete pwr
    Flag flag = pwr->flag;
    GAddr addr = pwr->addr;

    WorkRequest *parent = pwr->parent;
    epicAssert(parent);
    parent->lock();

    if (IsLocal(addr))
    {
      directory.lock(ToLocal(addr));
    }
    else
    {
      cache.lock(addr);
    }
    pwr->lock();
#ifdef DYNAMIC
    if (wr->status == 5732) { //版本不一致导致的失败问题,重新执行pwr
      //epicLog(LOG_WARNING, "got deadlock here");
      CacheLine * cline = cache.GetCLine(addr);
      MyAssert(cline->state == CACHE_TO_DIRTY || cline->state == CACHE_TO_SHARED);
      if (cline != nullptr) cache.ToInvalid(cline);
      cache.unlock(addr);
      parent->counter ++; //马上加入toserverequest
      pwr->unlock();
      parent->unlock();
      AddToServeLocalRequest(addr, parent);
      ProcessLocalRequest(parent); //直接重新执行一遍。
      delete wr;
      wr = nullptr;
      delete pwr;
      pwr = nullptr;
      return;
    }
#endif

    epicAssert(LOCK_FAILED == wr->status); // for now, only this should happen
    epicAssert((pwr->flag & LOCKED) && (pwr->flag & TRY_LOCK));
    epicLog(LOG_INFO, "wr->status = %d, op = %d", wr->status, pwr->op);
    epicLog(LOG_INFO, "write to %lx failed", pwr->addr);

    switch (pwr->op)
    {
    case WRITE:                 // local node (invalid)
    case WRITE_PERMISSION_ONLY: // local node (shared)
    case FETCH_AND_INVALIDATE:  // local and home node (dirty)
    case INVALIDATE:            // local and home node (shared)
    {
      if (WID(pwr->addr) == client->GetWorkerId())
      {                           // from home node, Case 4
        epicAssert(!wr->counter); // must be 0 since the home node has broadcast forward/invalidate req to other remote nodes
        epicAssert(WRITE == pwr->op || WRITE_PERMISSION_ONLY == pwr->op);
        pwr->counter = 0;
      }
      else
      {
        pwr->counter--;
      }

      /*
       * we cannot blindly erase the pending request for write
       * as there may be valid responses later
       */
      epicAssert(pwr->parent);
      epicAssert(pwr->parent->op == WLOCK);
      pwr->status = wr->status;
      pwr->parent->status = wr->status; // put the error status

      if (pwr->counter == 0)
      {
        // undo the directory/cache changes
        if (WRITE == pwr->op)
        {
          epicAssert(pwr->flag & CACHED);
          // cache.lock(pwr->addr);
          cache.ToInvalid(pwr->addr);
          // cache.unlock(pwr->addr);
        }
        else if (WRITE_PERMISSION_ONLY == pwr->op)
        {
          epicAssert(pwr->flag & CACHED);
          // cache.lock(pwr->addr);
          cache.UndoShared(pwr->addr);
          // cache.unlock(pwr->addr);
        }
        else if (FETCH_AND_INVALIDATE == pwr->op)
        {
          epicAssert(pwr->ptr == ToLocal(pwr->addr));
          // directory.lock(pwr->ptr);
          directory.UndoDirty(pwr->ptr);
          // directory.unlock(pwr->ptr);
        }
        else
        { // INVALIDATE
          epicAssert(pwr->ptr == ToLocal(pwr->addr));
          // directory.lock(pwr->ptr);
          directory.UndoShared(pwr->ptr);
          // directory.unlock(pwr->ptr);
        }

        pwr->unlock();
        // don't need to lock parent
        if (IsLocal(addr))
        {
          directory.unlock(ToLocal(addr));
        }
        else
        {
          cache.unlock(addr);
        }

        --pwr->parent->counter;
        epicAssert(pwr->parent->counter == 0); // lock is guaranteed to be only one block
        parent->unlock();                      // unlock earlier
        // Notify() should be called in the very last after all usage of parent,
        // since the app thread may exit the function and release the memory of parent
        Notify(pwr->parent);
        pwr->parent = nullptr;

        ProcessToServeRequest(pwr);
        int ret = ErasePendingWork(wr->id);
        epicAssert(ret);
        delete pwr;
        pwr = nullptr;
      }
      else
      {
        pwr->unlock();
        parent->unlock(); // unlock earlier
        // don't need to lock parent
        if (IsLocal(addr))
        {
          directory.unlock(ToLocal(addr));
        }
        else
        {
          cache.unlock(addr);
        }
      }
      // parent->unlock(); // @wentian: originally here
      break;
    }
    case WRITE_FORWARD:                 // home node
    case WRITE_PERMISSION_ONLY_FORWARD: // home node (shouldn't happen)
    {
      void *laddr = ToLocal(pwr->addr); // ToLocal(pwr->addr) != pwr->ptr as it is a forward msg
      // directory.lock(laddr);
      epicAssert(pwr->op == WRITE_FORWARD);
      epicAssert(IsLocal(pwr->addr));
      epicAssert(pwr->parent);
      epicAssert(pwr->pid == pwr->parent->id);
      DirEntry *entry = directory.GetEntry(ToLocal(pwr->addr));
      epicAssert(entry);
      epicAssert(directory.GetState(entry) == DIR_TO_DIRTY);
      directory.UndoDirty(entry);
      // directory.unlock(laddr);

      Client *lcli = FindClientWid(pwr->pwid);
      lcli->WriteWithImm(nullptr, nullptr, 0, pwr->pid); // ack the ownership change

      pwr->unlock();
      epicAssert(IsLocal(addr));
      directory.unlock(laddr);
      epicAssert(!pwr->status);
      parent->unlock();
      delete pwr->parent;
      pwr->parent = nullptr;
      ProcessToServeRequest(pwr);
      // TODO: verify this
      // we blindly erase the pending wr whose counter may be non-zero
      // following replies will be ignored since it cannot find pending wr in the pending list
      // ANSWER: it's ok here, since we are sure that we only have one response for these two ops
      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
      delete pwr;
      pwr = nullptr;
      break;
    }
    case INVALIDATE_FORWARD:
    {
      epicLog(LOG_INFO, "invalidate_forward failed");
      epicAssert(IsLocal(pwr->addr));
      pwr->counter--;

      /*
       * we cannot blindly erase the pending request for write
       * as there may be valid responses later
       */
      WorkRequest *parent = pwr->parent;
      epicAssert(parent);
      epicAssert((parent->flag & LOCKED) && (parent->flag & TRY_LOCK));
      pwr->status = wr->status;

      if (pwr->counter == 0)
      {
        void *laddr = ToLocal(pwr->addr);
        directory.UndoShared(laddr);

        pwr->unlock();
        epicAssert(IsLocal(addr));
        directory.unlock(ToLocal(addr));

        Client *pcli = FindClientWid(pwr->pwid);
        parent->status = wr->status; // put the error status
        parent->op = WRITE_REPLY;
        parent->counter = 0;
        epicAssert(parent->counter == 0);
        SubmitRequest(pcli, parent);
        parent->unlock();
        delete parent;
        parent = nullptr;
        pwr->parent = nullptr;

        ProcessToServeRequest(pwr);
        int ret = ErasePendingWork(wr->id);
        epicAssert(ret);
        delete pwr;
        pwr = nullptr;
      }
      else
      {
        pwr->unlock();
        parent->unlock();
        epicAssert(IsLocal(addr));
        directory.unlock(ToLocal(addr));
      }
      break;
    }
    default:
      epicLog(LOG_WARNING,
              "Unrecognized pending work request %d for WRITE_REPLY",
              pwr->op);
      exit(-1);
      break;
    }
  }
  else
  { // if not failed
    pwr->lock();
    pwr->counter += wr->counter;
    if (pwr->counter == 0)
    {
      pwr->flag |= REQUEST_DONE;
      pwr->unlock();
      ProcessPendingRequest(client, pwr);
    }
    else
    {
      pwr->unlock();
    }
  }
  delete wr;
  wr = nullptr;
}

/* add ergeda add */

void Worker::ProcessRemotePrivateRead(Client *client, WorkRequest *wr)
{
  // Just_for_test("ProcessRemotePrivateRead", wr);
  GAddr blk = TOBLOCK(wr->addr);
  if (IsLocal(blk))
  { // home_node = owner_node，直接用内存写，这里需要上锁吗？对directory上锁？
    directory.lock((void *)ToLocal(blk));
    client->WriteWithImm(wr->ptr, ToLocal(blk), wr->size, wr->id);
    directory.unlock((void *)ToLocal(blk));
  }
  else
  {
    cache.lock(blk);
    CacheLine *cline = cache.GetCLine(blk); // 考虑新建一个专门存单副本的数据结构？仿照这个cache可能会好些，永远不会被evict
    if (!cline)
    { // 相当于owner节点单副本却没数据？？
      epicLog(LOG_WARNING, "owner_node does not have data ?!?!");
    }
    else
    { // 存在单副本缓存数据，直接转发回去
      client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);
    }
    cache.unlock(blk);
  }

  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteRmRead(Client *client, WorkRequest *wr)
{
  // Just_for_test("ProcessRemoteRmRead", wr);
  if (!IsLocal(wr->addr))
  {
    epicLog(LOG_WARNING, "rm_read transfer to wrong node");
  }

  void *laddr = (void *)ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry *entry = directory.GetEntry(laddr);

  if (directory.InTransitionState(entry))
  { // 中间态不能传数据
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in Transition State %d",
            directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }

  entry->shared.push_back(client->ToGlobal(wr->ptr));
  client->WriteWithImm(wr->ptr, laddr, wr->size, wr->id);

  directory.unlock(laddr);

  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteSetCache(Client *client, WorkRequest *wr)
{

  //  epicLog(LOG_WARNING, "got to remote!");
  //  epicLog(LOG_WARNING, "cur_worker : %d", GetWorkerId());
  //  epicLog(LOG_WARNING, "are you kidding me");

  //  Just_for_test(wr);

  DataState Dstate = GetDataState(wr->flag);
  GAddr Owner = ((long long)(wr->arg) << 48);

  CreateDir(wr, Dstate, Owner);
  CreateCache(wr, Dstate);

  wr->op = SET_CACHE_REPLY;
  SubmitRequest(client, wr);

  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteRmWrite(Client *client, WorkRequest *wr)
{
  // Just_for_test("ProcessRemoteRmWrite", wr);
  if (!IsLocal(wr->addr))
  {
    epicLog(LOG_WARNING, "rm_write to wrong home_node");
  }

  GAddr blk = TOBLOCK(wr->addr); // wr->addr 不一定等于 blk
  void *laddr = (void *)ToLocal(wr->addr);
  directory.lock(ToLocal(blk));
  DirEntry *entry = directory.GetEntry(ToLocal(blk));
  if (entry == nullptr)
  {
    epicLog(LOG_WARNING, "rm_write no entry!");
  }

  if (directory.InTransitionState(entry))
  { // 中间态不能传数据
    AddToServeRemoteRequest(blk, client, wr);
    epicLog(LOG_INFO, "directory in Transition State %d",
            directory.GetState(entry));
    directory.unlock(ToLocal(blk));
    return;
  }

  entry->state = DIR_TO_SHARED;     // 中间态，等待所有副本节点确认
  memcpy(laddr, wr->ptr, wr->size); // 直接写入数据
  void *Gptr = (void *)wr->next;    // request_node cache的cline->line

  if (wr->flag & Add_list)
  { // 第一次访问，需加入shared_list
    entry->shared.push_back(client->ToGlobal(Gptr));
  }
  list<GAddr> &shared = directory.GetSList(entry);
  WorkRequest *lwr = new WorkRequest(*wr);
  lwr->lock();
  lwr->counter = 0;
  lwr->op = RM_FORWARD;
  lwr->addr = blk; // 之前是传buf过来，这里要改的是cache，得字节对齐
  lwr->parent = wr;

  lwr->id = GetWorkPsn();

  lwr->counter = shared.size();

  bool first = true;
  for (auto it = shared.begin(); it != shared.end(); it++)
  {
    Client *cli = GetClient(*it);
    if (cli == client)
    {
      lwr->counter--;
      continue;
    }
    epicLog(LOG_DEBUG, "invalidate forward (%d) cache from worker %d",
            lwr->op, cli->GetWorkerId());
    if (first)
    {
      AddToPending(lwr->id, lwr);
      first = false;
    }
    SubmitRequest(cli, lwr);
  }

  if (lwr->counter)
  { // 存在除了request_node之外的副本需要写。
    lwr->unlock();
    directory.unlock(ToLocal(blk));
    return; // return and wait for reply
  }
  else
  { // 不用等了，直接可以写回。
    lwr->unlock();
    delete lwr;
    lwr = nullptr;
    entry->state = DIR_SHARED;
    client->WriteWithImm(Gptr, ToLocal(blk), BLOCK_SIZE, wr->id);
    directory.unlock(ToLocal(blk));
    delete wr;
    wr = nullptr;
  }
}

void Worker::ProcessRemoteRmForward(Client *client, WorkRequest *wr)
{
  // Just_for_test("ProcessRemoteRmForward", wr);
  cache.lock(wr->addr);
  CacheLine *cline = nullptr;
  cline = cache.GetCLine(wr->addr);
  if (cline == nullptr)
  {
    epicLog(LOG_WARNING, "RmForward, owner_node do not have cache ??");
  }
  if (cache.InTransitionState(cline))
  {
    // epicLog(LOG_WARNING, "Deadlock rmforward and rmwrite");
    // deadlock(), 其他节点写的时候本地节点也提交了写请求传到Home_node。
    // 感觉这里deadlock问题不大，RM_DONE的时候判断下是否是TO_INVALID就好
  }
  else
  {
    cline->state = CACHE_TO_SHARED;
  }
  wr->op = RM_Done;
  AddToPending(-(wr->id), wr);

  client->WriteWithImm(nullptr, nullptr, 0, wr->id); // 告诉request-node已经写完了。
  cache.unlock(wr->addr);
  // Just_for_test("already write", wr);
}

void Worker::ProcessRemoteWeRead(Client *client, WorkRequest *wr)
{
  // Just_for_test("We_Read", wr);

  GAddr blk = TOBLOCK(wr->addr);

  void *laddr;
  if (IsLocal(blk))
    laddr = ToLocal(blk);
  else
    laddr = (void *)blk;

  directory.lock(laddr);
  DirEntry *entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry))
  {
    // to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in Transition State %d",
            directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }

  entry->shared.push_back(client->ToGlobal(wr->ptr));

  if (IsLocal(blk))
  { // home_node = owner_node
    client->WriteWithImm(wr->ptr, ToLocal(blk), wr->size, wr->id);
  }

  else
  {
    cache.lock(blk);
    CacheLine *cline = cache.GetCLine(blk);
    if (!cline)
    { // 相当于owner节点单副本却没数据？？
      epicLog(LOG_WARNING, "owner_node does not have data ?!?!");
    }
    else
    { // 存在单副本缓存数据，直接转发回去
      client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);
    }
    cache.unlock(blk);
  }
  directory.unlock(laddr);

  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteWeWrite(Client *client, WorkRequest *wr)
{

  // Just_for_test("We_Write", wr);

  GAddr blk = TOBLOCK(wr->addr);
  void *laddr;
  if (IsLocal(blk))
    laddr = ToLocal(blk);
  else
    laddr = (void *)blk;

  directory.lock(laddr);
  DirEntry *entry = directory.GetEntry(laddr);
  if (entry == nullptr)
  {
    epicLog(LOG_WARNING, "WeWrite no entry!");
  }

  if (directory.InTransitionState(entry))
  { // 中间态不能传数据
    AddToServeRemoteRequest(blk, client, wr);
    epicLog(LOG_INFO, "directory in Transition State %d",
            directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }

  entry->state = DIR_TO_SHARED; // 中间态，等待所有副本节点确认

  if (IsLocal(wr->addr))
  {                                               // home_node = owner_node
    memcpy(ToLocal(wr->addr), wr->ptr, wr->size); // 应该放到都无效完了再来写入？
  }
  else
  { // home_node != owner_node 更改cache
    cache.lock(blk);
    CacheLine *cline = nullptr;
    cline = cache.GetCLine(blk);
    if (cline == nullptr)
    {
      epicLog(LOG_WARNING, "owner_node do not have cache ??");
    }
    void *cur_addr = cline->line + GMINUS(wr->addr, blk);
    memcpy(cur_addr, wr->ptr, wr->size);
    cache.unlock(blk);
  }

  list<GAddr> &shared = directory.GetSList(entry);
  WorkRequest *lwr = new WorkRequest(*wr);
  lwr->lock();
  lwr->counter = 0;
  lwr->op = WE_INV;
  lwr->addr = blk; // 之前是传buf过来，这里要改的是cache，得字节对齐
  lwr->parent = wr;

  lwr->id = GetWorkPsn();

  lwr->counter = shared.size();

  bool first = true;
  for (auto it = shared.begin(); it != shared.end(); it++)
  {
    Client *cli = GetClient(*it);
    epicLog(LOG_DEBUG, "invalidate forward (%d) cache from worker %d",
            lwr->op, cli->GetWorkerId());
    if (first)
    {
      AddToPending(lwr->id, lwr);
      first = false;
    }
    SubmitRequest(cli, lwr);
  }

  if (lwr->counter)
  { // 存在除了request_node之外的副本需要写。
    lwr->unlock();
    directory.unlock(laddr);
    return; // return and wait for reply
  }
  else
  { // 不用等了，直接可以通知写操作完成。
    lwr->unlock();
    delete lwr;
    lwr = nullptr;
    entry->state = DIR_SHARED;
    client->WriteWithImm(nullptr, nullptr, 0, wr->id);
    directory.unlock(laddr);
    delete wr;
    wr = nullptr;
  }
}

void Worker::ProcessRemoteWeInv(Client *client, WorkRequest *wr)
{
  // Just_for_test("We_Inv", wr);
  cache.lock(wr->addr);
  CacheLine *cline = nullptr;
  cline = cache.GetCLine(wr->addr);
  if (cline == nullptr)
  {
    epicLog(LOG_WARNING, "WEINV, owner_node do not have cache ??");
    // 可能被evict掉了
  }
  else
  {
    if (cache.InTransitionState(cline))
    {
      // deadlock
    }
    else
    {
      cache.ToInvalid(cline);
      client->WriteWithImm(nullptr, nullptr, 0, wr->id);
    }
  }

  cache.unlock(wr->addr);

  delete wr;
  wr = nullptr;
}

/* add ergeda add */

/* add wpq add */
void Worker::ProcessRemoteWriteSharedRead(Client *client, WorkRequest *wr)
{

  GAddr blk = TOBLOCK(wr->addr);
  cache.lock(blk);
  CacheLine *cline = cache.GetCLine(blk);

  client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id); // reply to the local home node

  cache.unlock(blk);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteInitAcquire(Client *client, WorkRequest *wr)
{
  epicAssert(IsLocal(wr->addr));
  epicAssert(BLOCK_ALIGNED(wr->addr) && wr->size == BLOCK_SIZE);

  void *laddr = ToLocal(wr->addr);

  client->WriteWithImm(wr->ptr, laddr, wr->size, wr->id);

  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingWritesharedRead(Client *client, WorkRequest *wr)
{
  epicAssert(wr->parent);
  WorkRequest *parent = wr->parent;
  CacheLine *cline = nullptr;
  DirEntry *entry = nullptr;
  GAddr blk = TOBLOCK(wr->addr);

  parent->lock();

  directory.lock(ToLocal(wr->addr));
  entry = directory.GetEntry(ToLocal(wr->addr));
  epicAssert(entry);

  if (!(wr->flag & LOCKED))
  {
    GAddr pend = GADD(parent->addr, parent->size);
    GAddr end = GADD(wr->addr, wr->size);
    GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
    void *ls = (void *)((ptr_t)parent->ptr + GMINUS(gs, parent->addr));
    void *cs = (void *)((ptr_t)wr->ptr + GMINUS(gs, wr->addr));
    Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
    memcpy(ls, cs, len);
  }

  // update the cache or directory states
  if (!(wr->flag & REPEATED))
  {
    if ((wr->flag & CACHED))
    { // read is issued by the cache (remote memory)
      epicAssert(wr->op == READ);
      epicAssert(!IsLocal(wr->addr));
      cache.ToShared(cline);
    }
    else if (IsLocal(wr->addr))
    { // read is issued by local worker (local memory)
      epicAssert(wr->op == FETCH_AND_SHARED);
      directory.ToShared(entry, Gnullptr);
    }
    else
    {
      epicLog(LOG_WARNING, "unexpected!!!");
    }

    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
  }

  if (wr->flag & LOCKED)
  { // RLOCK
    epicAssert(
        !(wr->flag & NOT_CACHE) && wr->addr == blk && wr->size == BLOCK_SIZE);
    epicAssert(
        RLOCK == parent->op && 1 == parent->counter && 0 == parent->size);
    if (wr->flag & CACHED)
    { // RLOCK is issued by the cache (remote memory)
      epicAssert(wr->ptr == cline->line);
      epicAssert(!IsLocal(wr->addr));
      int ret = cache.RLock(cline, parent->addr);
      epicAssert(!ret); // first rlock must be successful
    }
    else if (IsLocal(wr->addr))
    { // RLock is issued by local worker (local memory)
      epicAssert(ToLocal(wr->addr) == wr->ptr);
      int ret;
      if (entry)
      {
        ret = directory.RLock(entry, ToLocal(parent->addr));
      }
      else
      {
        ret = directory.RLock(ToLocal(parent->addr)); // the dir entry may be deleted
      }
      epicAssert(!ret); // first rlock must be successful
    }
    else
    {
      epicLog(LOG_WARNING, "unexpected!!!");
    }
  }

  directory.unlock(ToLocal(wr->addr));

  if (--parent->counter == 0)
  { // read all the data
    parent->unlock();
    Notify(parent);
  }
  else
  {
    parent->unlock();
  }

  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

/* add wpq add */

void Worker::ProcessFlushToHome(Client *client, WorkRequest *wr)
{

  epicLog(LOG_DEBUG, "ProcessFlushToHome");
}