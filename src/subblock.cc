/* 
need change:
1.remote readcache
*/

#ifdef SUB_BLOCK
CacheLine * Cache::SetSubline(GAddr addr, int CurSize) {
  GAddr block = addr;
  CacheLine* cl = nullptr;
  if (caches.count(block)) {
    epicLog(LOG_WARNING, "cache line for gaddr %lx already exist in the cache", addr);
    cl = caches.at(block);
  } else {
    cl = new CacheLine();
    caddr ptr = worker->sb.sb_aligned_calloc(1,
                                            CurSize + CACHE_LINE_PREFIX);
    used_bytes += (CurSize + CACHE_LINE_PREFIX);
    //*(byte*) ptr = CACHE_INVALID;
    ptr = (byte*) ptr + CACHE_LINE_PREFIX;
    cl->line = ptr;
    cl->addr = block;
    cl->CacheSize = CurSize;
    caches[block] = cl;
  }
  return cl;
}

void Worker::ProcessRemoteSubRead(Client* client, WorkRequest* wr) {
    //Just_for_test("remote read", wr);
  MyAssert(IsLocal(wr->addr));
  void* laddr = ToLocal(wr->addr);

  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry)) {
    //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in Transition State %d",
        directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  if (directory.GetState(entry) != DIR_DIRTY) {  //it is shared or exclusively owned (Case 2)
    //add the lock support
    if (directory.IsBlockWLocked(entry)) {
      if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = READ_REPLY;
        directory.unlock(laddr);
        SubmitRequest(client, wr);
        delete wr;
        wr = nullptr;
      } else {
        //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
        AddToServeRemoteRequest(wr->addr, client, wr);
        directory.unlock(laddr);
      }
      epicLog(LOG_INFO, "addr %lx is exclusively locked by %d", ToGlobal(laddr),
          GetWorkerId());
      return;
    }

    //TODO: add the write completion check
    epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
    client->WriteWithImm(wr->ptr, ToLocal(wr->addr), wr->size, wr->id);
#ifdef SELECTIVE_CACHING
    if(!(wr->flag & NOT_CACHE)) {
#endif
      if (entry) {
        epicAssert(
            directory.GetState(entry) == DIR_UNSHARED
            || directory.GetState(entry) == DIR_SHARED);
        directory.ToShared(entry, client->ToGlobal(wr->ptr));
      } else {
        epicAssert(directory.GetState(entry) == DIR_UNSHARED);
        directory.ToShared(laddr, client->ToGlobal(wr->ptr));
      }
#ifdef SELECTIVE_CACHING
    }
#endif
    delete wr;
    wr = nullptr;
  } else {
    epicAssert(!directory.IsBlockLocked(entry));
    WorkRequest* lwr = new WorkRequest(*wr);
    lwr->counter = 0;
    lwr->op = READ_FORWARD;
    lwr->parent = wr;
    lwr->pid = wr->id;
    lwr->pwid = client->GetWorkerId();

    GAddr rc = directory.GetSList(entry).front();  //only one worker is updating this line
    Client* cli = GetClient(rc);
#ifdef SELECTIVE_CACHING
    if(!(wr->flag & NOT_CACHE)) {
      //intermediate state
      directory.ToToShared(entry);
      SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
    } else {
      SubmitRequest(cli, lwr);
    }
#else
    //intermediate state
    directory.ToToShared(entry);
    SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
#endif
  }
  directory.unlock(laddr);
}

void Worker::ProcessRemoteSubReadCache(Client* client, WorkRequest* wr) {
  Work op_orin = wr->op;
  bool deadlock = false;
  GAddr blk = wr->addr; //子块之后不能与block_Size对齐
  cache.lock(blk);
  CacheLine* cline = cache.GetSubCline(blk);
  if (!cline) {
    epicLog(LOG_FATAL, "Unexpected: cannot find an updated copy");
    wr->op = READ_REPLY;  //change op to the corresponding reply type
    wr->status = READ_ERROR;
    if (FETCH_AND_SHARED == op_orin) {
      SubmitRequest(client, wr);
    } else {  //READ_FORWARD
      SubmitRequest(client, wr);  //reply to the home node
      Client* cli = FindClientWid(wr->pwid);
      wr->id = wr->pid;
      SubmitRequest(cli, wr);  //reply to the local node
    }
  } else {
    if (cache.InTransitionState(cline->state)) {
      if (cline->state == CACHE_TO_DIRTY) {
        //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
        AddToServeRemoteRequest(wr->addr, client, wr);
        epicLog(LOG_INFO, "cache in transition state %d", cline->state);
        cache.unlock(blk);
        return;
      } else {
        //deadlock: this node wants to give up the ownership
        //meanwhile, another node wants to read
        epicLog(LOG_INFO, "!!!deadlock detected!!!\n");
        epicAssert(cline->state == CACHE_TO_INVALID);
        deadlock = true;
      }
    }

    //add the lock support
    if (cache.IsBlockWLocked(cline)) {
      if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = READ_REPLY;
        if (FETCH_AND_SHARED == op_orin) {
          SubmitRequest(client, wr);
        } else {  //READ_FORWARD
          SubmitRequest(client, wr);  //reply to the home node
          Client* cli = FindClientWid(wr->pwid);
          wr->id = wr->pid;
          SubmitRequest(cli, wr);  //reply to the local node
        }
        delete wr;
        wr = nullptr;
        cache.unlock(blk);
      } else {
        epicAssert(!deadlock);
        //we must unlock the cache/directory lock before calling the AddToServe[Remote]Request
        //as the lock acquire seq is fences -> directory/cache -> to_serve_local/remote_request/pending_works
        //the ProcessToServeRequest() breaks this rule
        //we copy the queue first and then release the to_serve.._request lock immediately
        //to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
        AddToServeRemoteRequest(wr->addr, client, wr);
        cache.unlock(blk);
      }
      epicLog(LOG_INFO, "addr %lx is exclusively locked by %d", blk,
          GetWorkerId());
      return;
    }

    //TODO: add the write completion check
    //can add it to the pending work and check it upon done
    if (op_orin == FETCH_AND_SHARED) {
#ifdef SELECTIVE_CACHING
      epicAssert(wr->size == BLOCK_SIZE && wr->addr == blk);
#endif
      MyAssert(BLOCK_ALIGNED(wr->addr) || wr->size < cline->CacheSize);
      client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);  //reply to the local home node
    } else {  //READ_FORWARD
      Client* cli = FindClientWid(wr->pwid);

#ifdef SELECTIVE_CACHING
      void* cs = (void*)((ptr_t)cline->line + GMINUS(wr->addr, blk));
      if(!(wr->flag & NOT_CACHE)) {
        epicAssert(wr->size == BLOCK_SIZE && wr->addr == blk && cs == cline->line);
      }
      epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
      cli->WriteWithImm(wr->ptr, cs, wr->size, wr->pid);  //reply to the local node
      if(!(wr->flag & NOT_CACHE)) {
        epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
        client->WriteWithImm(client->ToLocal(blk), cline->line, BLOCK_SIZE, wr->id);  //writeback to home node
      }
#else
      MyAssert(BLOCK_ALIGNED(wr->addr) || wr->size < cline->CacheSize);
      cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid);  //reply to the local node
      client->WriteWithImm(client->ToLocal(blk), cline->line, cline->CacheSize,
          wr->id);  //writeback to home node
#endif
    }

#ifdef SELECTIVE_CACHING
    if(!(wr->flag & NOT_CACHE)) {
#endif
      //TOOD: add below to the callback function
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

void Worker::ProcessRemoteSubWrite(Client* client, WorkRequest* wr) {
  Work op_orin = wr->op;
  MyAssert(IsLocal(wr->addr));  //I'm the home node
  void* laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  DirState state = directory.GetState(entry);
  if (directory.InTransitionState(state)) {
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "Directory in Transition State %d", state);
    directory.unlock(laddr);
    return;
  }
  if (state != DIR_DIRTY) {
    //add the lock support
    if (directory.IsBlockLocked(entry)) {
      epicAssert((directory.IsBlockWLocked(entry) && state == DIR_UNSHARED)
          || !directory.IsBlockWLocked(entry));
      if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = WRITE_REPLY;
        wr->counter = 0;
        SubmitRequest(client, wr);
        delete wr;
        wr = nullptr;
        directory.unlock(laddr);
      } else {
        AddToServeRemoteRequest(wr->addr, client, wr);
        directory.unlock(laddr);
      }
      epicLog(LOG_INFO, "addr %lx is locked by %d", ToGlobal(laddr),
          GetWorkerId());
      return;
    }

    if (state == DIR_SHARED) {
      //change the invalidate strategy (home node accepts invalidation responses)
      //in order to simply the try_lock failed case
      list<GAddr>& shared = directory.GetSList(entry);
      WorkRequest* lwr = new WorkRequest(*wr);
#ifdef SELECTIVE_CACHING
      if(wr->flag & NOT_CACHE) {
        epicAssert(wr->size <= BLOCK_SIZE);
        lwr->addr = TOBLOCK(wr->addr);
        lwr->size = BLOCK_SIZE;
        lwr->ptr = (void*)((ptr_t)lwr->ptr - GMINUS(wr->addr, lwr->addr)); //not necessary
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
      for (auto it = shared.begin(); it != shared.end(); it++) {
        Client* cli = GetClient(*it);
        if (cli == client) {
          epicAssert(op_orin == WRITE_PERMISSION_ONLY);
          lwr->counter--;
          continue;
        }
        epicLog(LOG_DEBUG, "invalidate forward (%d) cache from worker %d",
            lwr->op, cli->GetWorkerId());
        if (first) {
          AddToPending(lwr->id, lwr);
          first = false;
        }
        SubmitRequest(cli, lwr);
        //lwr->counter++;
      }

      if (lwr->counter) {
        lwr->unlock();
        directory.ToToDirty(entry);
        directory.unlock(laddr);
        return;  //return and wait for reply
      } else {
        lwr->unlock();
        epicAssert(op_orin == WRITE_PERMISSION_ONLY);
        delete lwr;
        lwr = nullptr;
      }
    } else {  //DIR_UNSHARED
#ifdef SELECTIVE_CACHING
      if(wr->flag & NOT_CACHE) {
#ifdef GFUNC_SUPPORT
        if(wr->flag & GFUNC) {
          epicAssert(wr->gfunc);
          epicAssert(TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size-1)));
          void* laddr = ToLocal(wr->addr);
          wr->gfunc(laddr, wr->arg);
        } else {
#endif
          memcpy(ToLocal(wr->addr), wr->ptr, wr->size);
#ifdef GFUNC_SUPPORT
        }
#endif
      } else {
#endif
        if (WRITE == op_orin) {
          epicLog(LOG_DEBUG, "write the data (size = %ld) to destination",
              wr->size);
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->Write(wr->ptr, laddr, wr->size);
        } else {  //WRITE_PERMISSION_ONLY
          epicAssert(state == DIR_UNSHARED);
          //deadlock: one node (Node A) wants to update its cache from shared to dirty,
          //but at the same time, the home nodes invalidates all its shared copy (due to a local write)
          //currently, dir_state == dir_unshared (after pend the request because it was dir_to_unshared)
          //solution: Node A acts as it is still a shared copy so that the invalidation can completes,
          //after which, home node processes the pending list and change the WRITE_PERMISSION_ONLY to WRITE
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
    if(!(wr->flag & NOT_CACHE)) {
#endif

      //we can safely change the directory as we've already transfered the data to the local node
      // logging
      logOwner(client->GetWorkerId(), wr->addr);
      if (entry) {
        directory.ToDirty(entry, client->ToGlobal(wr->ptr));
      } else {
        directory.ToDirty(laddr, client->ToGlobal(wr->ptr));  //entry is null
      }

#ifdef SELECTIVE_CACHING
    }
#endif
    delete wr;
    wr = nullptr;
  } else {  //Case 4
    epicAssert(!directory.IsBlockLocked(entry));
    WorkRequest* lwr = new WorkRequest(*wr);
#ifdef SELECTIVE_CACHING
    if (wr->flag & NOT_CACHE) {
      epicAssert(wr->size <= BLOCK_SIZE);
      lwr->addr = TOBLOCK(wr->addr);
      lwr->size = BLOCK_SIZE;
      lwr->ptr = (void*)((ptr_t)lwr->ptr - GMINUS(wr->addr, lwr->addr)); //not necessary
    }
#endif
    lwr->counter = 0;
    if (WRITE == op_orin || WLOCK == op_orin) {
      lwr->op = WRITE_FORWARD;
    } else if (WRITE_PERMISSION_ONLY == op_orin) {
      //deadlock: WRITE_PERMISSION_ONLY shouldn't co-exist with DIR_DIRTY state
      //there must be a race where one nodes (Node A) tries to update its cache from shared to dirty,
      //while another node (Node B) writes the data before that node
      //solution: Node A replies as its cache line is shared, and home node changes it to WRITE_FORWARD
      //lwr->op = WRITE_PERMISSION_ONLY_FORWARD;
      lwr->op = WRITE_FORWARD;
    }
    lwr->parent = wr;
    lwr->pid = wr->id;
    lwr->pwid = client->GetWorkerId();

    GAddr rc = directory.GetSList(entry).front();  //only one worker is updating this line
    Client* cli = GetClient(rc);

    /* add xmx add */
    if (op_orin == WRITE) racetime += 1;
    /* add xmx add */

    //intermediate state
    directory.ToToDirty(entry);
    SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
  }
  directory.unlock(laddr);
}

void Worker::ProcessRemoteSubWriteCache(Client* client, WorkRequest* wr) {
  epicAssert(wr->op != WRITE_PERMISSION_ONLY_FORWARD);  //this cannot happen
  Work op_orin = wr->op;
  bool deadlock = false;
  MyAssert(!IsLocal(wr->addr));  //I'm not the home node
  //we hold an updated copy of the line (WRITE_FORWARD: Case 4)
  GAddr to_lock = wr->addr;
  cache.lock(to_lock);
  CacheLine* cline = cache.GetSubCline(wr->addr);
  if (!cline) {
    if (INVALIDATE == op_orin || INVALIDATE_FORWARD == op_orin) {
      //this should because of cache line eviction from shared to invalid
      //so we reply as if it is shared
      deadlock = true;

      //TODO: add the write completion check
      //can add it to the pending work and check it upon done
      if (wr->op == INVALIDATE) {  //INVALIDATE
        client->WriteWithImm(nullptr, nullptr, 0, wr->id);
      } else {  //INVALIDATE_FORWARD
        //			Client* cli = FindClientWid(wr->pwid);
        //			cli->WriteWithImm(nullptr, nullptr, 0, wr->pid); //reply the new owner
        //			epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
        //      after change the invalidate_forward strategy
        client->WriteWithImm(nullptr, nullptr, 0, wr->id);
        epicLog(LOG_DEBUG, "send to %d with id %d", client->GetWorkerId(),
            wr->id);
      }
    } else {
      epicLog(LOG_FATAL, "Unexpected: cannot find an updated copy");
      wr->op = WRITE_REPLY;  //change op to the corresponding reply type
      wr->status = WRITE_ERROR;
      if (INVALIDATE == op_orin || FETCH_AND_INVALIDATE == op_orin) {
        SubmitRequest(client, wr);
      } else if (INVALIDATE_FORWARD == op_orin) {
        //			Client* cli = FindClientWid(wr->pwid);
        //			wr->id = wr->pid;
        //			SubmitRequest(cli, wr);
        SubmitRequest(client, wr);
      } else {  //WRITE_FORWARD or WRITE_PERMISSION_ONLY_FORWARD
        SubmitRequest(client, wr);
        Client* cli = FindClientWid(wr->pwid);
        wr->id = wr->pid;
        SubmitRequest(cli, wr);
      }
    }
    delete wr;
    wr = nullptr;
  } else {
    if (cache.InTransitionState(cline->state)) {
      /*
       * deadlock, since the responding node must just change its cache state
       * and send request to home node,
       * who was not notified of the change and sent an invalidate/forward request.
       * How to solve?
       * there are two causes: cache from shared to dirty (ToDirty State)
       * cache from dirty to invalid (ToInvalid state)
       */
      if ((INVALIDATE == wr->op || INVALIDATE_FORWARD == wr->op)
          && cline->state == CACHE_TO_DIRTY) {
        //deadlock case 1
        epicLog(LOG_INFO, "!!!deadlock detected!!!");
        deadlock = true;
      } else {
        if (cline->state == CACHE_TO_INVALID) {
          //deadlock case 2
          epicLog(LOG_INFO, "!!!deadlock detected!!!");
          deadlock = true;
        } else {
          AddToServeRemoteRequest(wr->addr, client, wr);
          epicLog(LOG_INFO, "cache in transition state %d", cline->state);
          cache.unlock(to_lock);
          return;
        }
      }
    }

    //add the lock support
    if (cache.IsBlockLocked(cline)) {
      if (wr->flag & TRY_LOCK) {  //reply directly with lock failed
        epicAssert(wr->flag & LOCKED);
        wr->status = LOCK_FAILED;
        wr->op = WRITE_REPLY;
        if (INVALIDATE == op_orin || FETCH_AND_INVALIDATE == op_orin) {
          SubmitRequest(client, wr);
        } else if (INVALIDATE_FORWARD == op_orin) {
          //				Client* cli = FindClientWid(wr->pwid);
          //				wr->id = wr->pid;
          //				SubmitRequest(cli, wr);
          SubmitRequest(client, wr);
        } else {  //WRITE_FORWARD or WRITE_PERMISSION_ONLY_FORWARD
          SubmitRequest(client, wr);
          Client* cli = FindClientWid(wr->pwid);
          wr->id = wr->pid;
          SubmitRequest(cli, wr);
        }
        cache.unlock(to_lock);
        delete wr;
        wr = nullptr;
        return;
      } else {
        //deadlock case 3
        //if it is rlocked, and in deadlock status (in transition state from shared to dirty)
        //we are still safe to act as it was in shared state and ack the invalidation request
        //because the intransition state will block other r/w requests
        //until we get replies from the home node (then WRITE_PERMISSION_ONLY has
        //been changed to WRITE by the home node as agreed)
        if (!deadlock) {
          AddToServeRemoteRequest(wr->addr, client, wr);
          epicLog(LOG_INFO, "addr %lx is locked by %d", wr->addr,
              GetWorkerId());
          cache.unlock(to_lock);
          return;
        } else {
          epicLog(LOG_WARNING, "Deadlock detected");
        }
      }
    }

    //TODO: add the write completion check
    //can add it to the pending work and check it upon done
    if (wr->op == FETCH_AND_INVALIDATE) {  //FETCH_AND_INVALIDATE
      epicAssert(cache.IsDirty(cline) || cache.InTransitionState(cline));
      if (deadlock) {
        epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
        client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);
        delete wr;
        wr = nullptr;
      } else {
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

    } else if (wr->op == INVALIDATE) {  //INVALIDATE
      epicAssert(!cache.IsDirty(cline));
      client->WriteWithImm(nullptr, nullptr, 0, wr->id);
      //TOOD: add below to the callback function
      if (!deadlock)
        cache.ToInvalid(cline);
      delete wr;
      wr = nullptr;
    } else if (wr->op == INVALIDATE_FORWARD) {  //INVALIDATE_FORWARD
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
    } else {  //WRITE_FORWARD
      Client* cli = FindClientWid(wr->pwid);
      if (deadlock) {
#ifdef SELECTIVE_CACHING
        if(wr->flag & NOT_CACHE) {
          //client->WriteWithImm(wr->ptr, cline->line, wr->size, wr->id);  //transfer ownership
          //fix bug here (wr->ptr is not the same as ToLocal(wr->addr)
          //and here we write the dirty data back to the home node rather than
          //the local node requesting the data
          epicAssert(BLOCK_ALIGNED(wr->addr));
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->WriteWithImm(client->ToLocal(wr->addr), cline->line, wr->size, wr->id);  //transfer ownership
        } else {
#endif
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid);  //reply the new owner
          epicLog(LOG_DEBUG, "send to %d with pid %d", wr->pwid, wr->pid);
          client->WriteWithImm(nullptr, nullptr, 0, wr->id);  //transfer ownership
#ifdef SELECTIVE_CACHING
        }
#endif
        delete wr;
        wr = nullptr;
      } else {
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
        if(wr->flag & NOT_CACHE) {
          epicAssert(BLOCK_ALIGNED(wr->addr));
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          client->WriteWithImm(client->ToLocal(wr->addr), cline->line, wr->size, orig_id, wr->id, true);  //transfer ownership
        } else {
#endif
          epicAssert(BLOCK_ALIGNED(wr->addr) || wr->size < BLOCK_SIZE);
          cli->WriteWithImm(wr->ptr, cline->line, wr->size, wr->pid, wr->id,
              true);  //reply the new owner
          client->WriteWithImm(nullptr, nullptr, 0, orig_id);  //transfer ownership
#ifdef SELECTIVE_CACHING
        }
#endif
      }
    }
  }
  cache.unlock(to_lock);
}
#endif

#ifdef DYNAMIC
void Worker::StartChange(GAddr addr, DataState CurState) {
  //epicLog(LOG_WARNING, "start change subblock");
  void * laddr = ToLocal(addr);
  //directory.lock(laddr); //在pending request里就锁住，保证原子性
  DirEntry * Entry = directory.GetEntry(laddr);
  DirState state = directory.GetState(Entry);
  list<GAddr>& shared = directory.GetSList(Entry);
  MyAssert(state == DIR_DIRTY || state == DIR_SHARED);
  if (state == DIR_UNSHARED) {
    directory.ToToUnShared(Entry);
    directory.unlock(laddr);
    ChangeDir(addr, CurState);
    return;
  }

  WorkRequest* lwr = new WorkRequest();
  lwr->flag = CheckChange;
  lwr->flag |= (RevGetstate(CurState) ); //将要转化为什么类型的子块
  lwr->counter = 0;
  lwr->op = state == DIR_DIRTY ? FETCH_AND_INVALIDATE : INVALIDATE;
  lwr->addr = addr;
  lwr->size = Entry->MySize;
  lwr->ptr = laddr;
  lwr->id = GetWorkPsn();
  lwr->counter = shared.size();

  directory.ToToUnShared(Entry);

  AddToPending(lwr->id, lwr);
  for (auto it = shared.begin(); it != shared.end(); it++) {
    Client* cli = GetClient(*it);
    SubmitRequest(cli, lwr);
  }
  directory.unlock(laddr);
}

void Worker::ChangeDir(GAddr addr, DataState CurState) {
  //epicLog(LOG_WARNING, "start changedir");
  MyAssert(IsLocal(addr));
  void * laddr = ToLocal(addr);

  directory.lock(laddr);
  DirEntry * Entry = directory.GetEntry(laddr);
  MyAssert(Entry != nullptr);

  if (CurState == WRITE_SHARED) {
    int PreSize = Entry->MySize;
    int AfterSize = PreSize / 2;
    int CurMetaVersion = Entry->MetaVersion + 1;

    directory.lock(laddr + AfterSize);
    directory.CreateEntry(laddr + AfterSize, MSI, 0);
    DirEntry * Next = directory.GetEntry(laddr + AfterSize);
    directory.DirInit(Next, CurState, AfterSize, CurMetaVersion);
    directory.DirInit(Entry, CurState, AfterSize, CurMetaVersion);

    directory.ToToUnShared(Next);
    directory.ToToUnShared(Entry); //md 罪魁祸首，调一天
  }

  WorkRequest * wr = new WorkRequest();
  
  wr->lock();

  wr->addr = addr;
  wr->op = CHANGE;
  wr->id = GetWorkPsn();
  wr->flag = RevGetstate(CurState);
  wr->counter = (int)widCliMapWorker.size();
  
  bool first = true;
  for (auto& entry: widCliMapWorker) {
    if (entry.first != this->GetWorkerId()) {
      if (first == true) {
        first = false;
        AddToPending(wr->id, wr);
      }
      this->SubmitRequest(entry.second, wr);
    }
    else wr->counter -= 1;
  }

  if (CurState == WRITE_SHARED) {
    directory.unlock(laddr + Entry->MySize);
  }

  wr->unlock();
  directory.unlock(laddr);
}

void Worker::ProcessRemoteChange(Client * client, WorkRequest * wr) {
  DataState Curs = GetDataState(wr->flag);
  void * laddr = (void *)(wr->addr);
  if (Curs == WRITE_SHARED) {
    directory.lock(laddr);
    DirEntry * Entry = directory.GetEntry(laddr);
    if (Entry == nullptr) { //本地无副本，可以直接回复确认改变完毕
      client->WriteWithImm(nullptr, nullptr, 0, wr->id);
      directory.unlock(laddr);
      delete wr;
      wr = nullptr;
      return;
    }
    if (directory.InTransitionState(Entry) ) { // 进入这个判断就可能是已经死锁了,但死锁不影响
      epicLog(LOG_WARNING, "node's directory intransition");
      // AddToServeRemoteRequest(wr->addr, client, wr);
      // directory.unlock(laddr);
      // return;
    }

    int PreSize = Entry->MySize;
    int AfterSize = PreSize / 2;
    int CurVersion = Entry->MetaVersion + 1;

    directory.lock(laddr + AfterSize);
    directory.CreateEntry( laddr + AfterSize, DataState::MSI, 0);
    DirEntry * Next = directory.GetEntry(laddr + AfterSize);
    directory.DirInit(Next, Curs, AfterSize, CurVersion);
    directory.DirInit(Entry, Curs, AfterSize, CurVersion);

    client->WriteWithImm(nullptr, nullptr, 0, wr->id);

    directory.unlock(laddr + AfterSize);
    directory.unlock(laddr);
  }
}

void Worker::ProcessPendingChange(Client * client, WorkRequest * wr) {
  wr->lock();

  if ( (--wr->counter) == 0) {
    void * laddr = ToLocal(wr->addr);
    directory.lock(laddr);
    DirEntry * Entry = directory.GetEntry(laddr);
    directory.lock(laddr + Entry->MySize);
    DirEntry * Next = directory.GetEntry(laddr + Entry->MySize);
    Next->state = DIR_UNSHARED;
    directory.unlock(laddr + Entry->MySize);
    Entry->state = DIR_UNSHARED;
    int CurSize = Entry->MySize;
    directory.unlock(laddr);

    wr->unlock();
    ProcessToServeRequest(wr);
    wr->addr += CurSize;
    ProcessToServeRequest(wr);
    delete wr;
    wr = nullptr;
    return ;
  }
  wr->unlock();
}
#endif

#ifdef B_I
void Worker::UpdateVersion(DirEntry * Entry, GAddr addr) {
  if (Entry->version_list.size() > 10) {
    printf ("already over 10 !!!\n");
  }
  while (!(Entry->version_list.empty()) ) {
    auto it = Entry->version_list.begin();
    BI_dir * Cur_Bientry = (*it);
    if ( (Entry->version_list.size() > Max_version) || (GMINUS(get_time(), Cur_Bientry->Timestamp) > Max_timediff) ) {
      uint64 Cur_timestamp = GMINUS(get_time(), Cur_Bientry->Timestamp);

      for (auto it = Cur_Bientry->shared.begin(); it != Cur_Bientry->shared.end(); ++it) {
        WorkRequest * lwr = new WorkRequest();
        lwr->addr = TOBLOCK(addr);
        lwr->op = BI_INV;
        lwr->arg = Cur_Bientry->Timestamp;
        Client* cli = GetClient(*it);
        SubmitRequest(cli, lwr); //通知Invalidate过期的副本（像拥有这个副本的节点）
        delete lwr;
        lwr = nullptr;
      }
      directory.Delete_BIdirbegin(Entry);
      continue;
    }
    break;
  }
}
void Worker::ProcessRemoteBIWrite(Client * client, WorkRequest * wr) {
  void * Startcopy = ToLocal(wr->addr); //写开始的位置
  void * laddr = ToLocal(TOBLOCK(wr->addr)); //TODO： 在wr中用另外的变量存储目录位置，以兼容subblock
  directory.lock(laddr);
  DirEntry * Entry = directory.GetEntry(laddr);
  memcpy (Startcopy, wr->ptr, wr->size); //直接写入内存
  BI_dir * Last_BIentry = directory.getlastbientry(Entry);
  if (Last_BIentry->shared.empty()) { //最后一个版本（即最新版本没有人共享，就可以直接改）
    Last_BIentry->Timestamp = get_time();
  }
  else {
    BI_dir * BI_entry = directory.Create_BIdir();
    directory.Add_BIdir(Entry, BI_entry);
  }

  UpdateVersion(Entry, wr->addr);
  client->WriteWithImm(nullptr, nullptr, 0, wr->id);
  directory.unlock(laddr);

  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteBIInv(Client * client, WorkRequest * wr) {
  uint64 Cur_timestamp = wr->arg;
  cache.lock(wr->addr);
  CacheLine * cline = cache.GetCLine(wr->addr);
  if (cline == nullptr) {
    epicLog(LOG_WARNING, "no cache exist, not usual");
    delete wr;
    wr = nullptr;
    return;
  }
  if (cline->Timestamp == Cur_timestamp) {
    cline->state = CACHE_INVALID;
  }
  cache.unlock(wr->addr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteBIRead(Client * client, WorkRequest * wr) {
  void * laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  client->WriteWithImm(wr->ptr, laddr, wr->size, wr->id);
  wr->op = BI_INFORM;
  DirEntry * Entry = directory.GetEntry(laddr);
  BI_dir * BI_Entry = directory.getlastbientry(Entry);
  BI_Entry->shared.push_back(client->ToGlobal(wr->ptr));
  wr->arg = BI_Entry->Timestamp; //版本信息
  SubmitRequest(client, wr);
  directory.unlock(laddr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteBIInform(Client * client, WorkRequest * wr) {
  cache.lock(wr->addr);
  CacheLine * cline = cache.GetCLine(wr->addr);
  if (cline != nullptr) {
    cline->Timestamp = wr->arg;
  }
  cache.unlock(wr->addr);
}

void Worker::ProcessPendingBIRead(Client * client, WorkRequest * wr) {
  WorkRequest* parent = wr->parent;
  parent->lock();

  cache.lock(wr->addr);
  GAddr pend = GADD(parent->addr, parent->size);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
  void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
  void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
  Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
  memcpy(ls, cs, len);
  CacheLine * cline = cache.GetCLine(wr->addr);
  cline->state = CACHE_DIRTY;
  cache.unlock(wr->addr);

  if ( (--parent->counter) == 0) {  //read all the data
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  } else {
    parent->unlock();
  }

  int ret = ErasePendingWork(wr->id);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingBIWrite(Client * client, WorkRequest * wr) {
  WorkRequest* parent = wr->parent;
  parent->lock();
  if ( (--parent->counter) == 0) {  //read all the data
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  } else {
    parent->unlock();
  }

  int ret = ErasePendingWork(wr->id);
  delete wr;
  wr = nullptr;
}
#endif