// Copyright (c) 2018 The GAM Authors
#include "worker.h"

int Worker::ProcessLocalRead(WorkRequest *wr)
{
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));

  if (!(wr->flag & FENCE))
  {
    Fence *fence = fences_.at(wr->fd);
    fence->lock();
    if (unlikely(IsMFenced(fence, wr)))
    {
      AddToFence(fence, wr);
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }

  if (likely(IsLocal(wr->addr)))
  {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    GAddr end = GADD(start, wr->size);
    wr->lock();
    /*
     * we increase it by 1 before we push to the to_serve_local_request queue
     * so we have to decrease by 1 again
     */
    if (wr->flag & TO_SERVE)
    {
      wr->counter--;
    }
    for (GAddr i = start_blk; i < end;)
    {
      GAddr nextb = BADD(i, 1);
      void *laddr = ToLocal(i);

      directory.lock(laddr);
      DirEntry *entry = directory.GetEntry(laddr);
      DirState s = directory.GetState(entry);
      if (unlikely(directory.InTransitionState(s)))
      {
        epicLog(LOG_INFO, "directory in transition state when local read %d",
                s);
        // we increase the counter in case
        // we false call Notify()
        wr->counter++;
        AddToServeLocalRequest(i, wr);
        directory.unlock(laddr);
        wr->unlock();
        wr->is_cache_hit_ = false;
        return IN_TRANSITION;
      }

      /* add ergeda add */

      DataState Ds = directory.GetDataState(entry);
      GAddr Cur_owner = directory.GetOwner(entry);

      if (Ds != MSI)
      {
        if (Ds == ACCESS_EXCLUSIVE)
        {
          if (WID(Cur_owner) == GetWorkerId())
          { // owner == home_node
            GAddr gs = i > start ? i : start;
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            memcpy(ls, ToLocal(gs), len); // 直接复制就行
          }
          else
          { // owner != home_node, send to owner_node
            // 新建一个cache来存储，回来之后删掉。
            // Just_for_test("Local Read", wr);
            cache.lock(i);
            CacheLine *cline = nullptr;
            cline = cache.SetCLine(i); // temporary , just for write_with_imm
            cache.unlock(i);

            WorkRequest *lwr = new WorkRequest(*wr);
            lwr->counter = 0;
            Client *cli = GetClient(Cur_owner);
            lwr->op = JUST_READ;
            lwr->addr = i;
            lwr->size = BLOCK_SIZE;
            lwr->ptr = cline->line;
            lwr->parent = wr;

            wr->counter++;
            wr->is_cache_hit_ = false;
            SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
          }

          directory.unlock(laddr);
          i = nextb;
          continue;
        }

        else if (Ds == READ_MOSTLY)
        { // read_mostly, 这种情况直接读就行，home_node有最新数据
          GAddr gs = i > start ? i : start;
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          memcpy(ls, ToLocal(gs), len); // 直接复制就行

          directory.unlock(laddr);
          i = nextb;
          continue;
        }

        else if (Ds == WRITE_EXCLUSIVE)
        { // shared_list 什么的都转移到owner node去了，这里就是普通节点罢了
          if (WID(Cur_owner) == GetWorkerId())
          { // home_node = owner_node,直接读
            GAddr gs = i > start ? i : start;
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            memcpy(ls, ToLocal(gs), len); // 直接复制就行
          }
          else
          { // 否则以owner节点为home_node
            cache.lock(i);
            CacheLine *cline = nullptr;
            if (cline = cache.GetCLine(i))
            { // 有缓存
              CacheState state = cline->state;
              if (unlikely(cache.InTransitionState(state)))
              { // transition state
                epicLog(LOG_INFO, "in transition state while cache read/write(%d)", wr->op);

                wr->counter++;
                wr->unlock();
                if (wr->flag & ASYNC)
                {
                  if (!wr->IsACopy())
                  {
                    wr = wr->Copy();
                  }
                }
                AddToServeLocalRequest(i, wr);
                cache.unlock(i);
                return 1;
              }

              GAddr gs = i > start ? i : start;
              void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
              int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
              memcpy(ls, cline->line, len); // 直接复制就行
            }
            else
            { // 无缓存,需要去owner_node取数据
              WorkRequest *lwr = new WorkRequest(*wr);
              // newcline++;
              cline = cache.SetCLine(i);
              lwr->op = WE_READ;
              lwr->counter = 0;
              lwr->flag |= CACHED;
              lwr->addr = i;
              lwr->size = BLOCK_SIZE;
              lwr->ptr = cline->line;
              wr->is_cache_hit_ = false;
              if (wr->flag & ASYNC)
              {
                if (!wr->IsACopy())
                {
                  wr->unlock();
                  wr = wr->Copy();
                  wr->lock();
                }
              }
              lwr->parent = wr;
              wr->counter++;
              if (READ == wr->op)
              {
                cache.ToToShared(cline);
              }
              Client *cli = GetClient(Cur_owner);
              SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
            }
            cache.unlock(i);
          }
          directory.unlock(laddr);
          i = nextb;
          continue;
        }
        
        else if (Ds == WRITE_SHARED)
        {
          /* add wpq add*/
          int subblock_num = directory.GetSubBlockNum(entry, wr->addr);

          int subblock_owner = directory.GetSubBlockOwner(entry, subblock_num);
          GAddr subblock_addr = directory.GetSubBlockAddr(entry, subblock_owner);

          /* add wpq add */
          int offset = wr->addr - TOBLOCK(wr->addr);
          epicLog(LOG_PQ, "Ds = %d, offset = %d,subblock_num = %d, subblock_owner = %d",
                  Ds, offset, subblock_num, subblock_owner);
          epicLog(LOG_PQ, "3 wr->addr=%lx,start=%lx", wr->addr, start);

          if (subblock_owner == GetWorkerId())
          {
            epicLog(LOG_PQ, "subblock_owner == GetWorkerId()");
            if (i < start)
            {
              epicLog(LOG_PQ, "i<start");
            }
            if (nextb > end)
            {
              epicLog(LOG_PQ, "nextb>end");
            }

            GAddr gs = i > start ? i : start;
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            epicLog(LOG_PQ, "gs=%lx, ls=%lx, len=%d", gs, ls, len);

            memcpy(ls, ToLocal(gs), len); // 直接复制就行
          }
          else
          {
            epicLog(LOG_PQ, "subblock_owner != GetWorkerId()");
            WorkRequest *lwr = new WorkRequest(*wr);
            lwr->counter = 0;

            Client *cli = GetClient(subblock_addr);
            lwr->op = writeshared_READ;
            lwr->addr = i;
            lwr->size = BLOCK_SIZE;
            lwr->ptr = laddr;
            lwr->parent = wr;
            wr->counter++;
            wr->is_cache_hit_ = false;

            SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
          }
          directory.unlock(laddr);
          i = nextb;
          continue;
        }
        
        else if (Ds == RC_WRITE_SHARED)
        {
          epicLog(LOG_DEBUG, "RC_WRITE_SHARED,wr->addr=%lx\n", wr->addr);

          GAddr gs = i > start ? i : start;
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          epicLog(LOG_DEBUG, "gs=%lx, ls=%lx, len=%d", gs, ls, len);

          memcpy(ls, ToLocal(gs), len); // 直接复制就行

          directory.unlock(laddr);
          i = nextb;
          continue;
        }
        /* add wpq add */
      }
      /* add ergeda add */

      if (unlikely(s == DIR_DIRTY))
      {
        WorkRequest *lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        GAddr rc = directory.GetSList(entry).front(); // only one worker is updating this line
        Client *cli = GetClient(rc);
        lwr->op = FETCH_AND_SHARED;
        lwr->addr = i;
        lwr->size = BLOCK_SIZE;
        lwr->ptr = laddr;
        lwr->parent = wr;
        wr->counter++;
        wr->is_cache_hit_ = false;
        // intermediate state
        epicAssert(s != DIR_TO_SHARED);
        epicAssert(!directory.IsBlockLocked(entry));
        directory.ToToShared(entry, rc);
        SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
      }
      else
      {
        GAddr gs = i > start ? i : start;
        void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        memcpy(ls, ToLocal(gs), len);
      }
      directory.unlock(laddr);
      i = nextb;
    }
    if (unlikely(wr->counter))
    {
      wr->unlock();
      return REMOTE_REQUEST;
    }
    else
    {
      wr->unlock();
    }
  }
  else
  {
    int ret = cache.Read(wr);
    if (ret)
      return REMOTE_REQUEST;
  }

#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE)
  {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr))
    {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
  else
  {
    /**
     * In this case, the read request is running in the app thread and
     * is fulfilled in the first trial (i.e., * chache hit)
     */
    epicAssert(wr->is_cache_hit_);
    if (IsLocal(wr->addr))
    {
      ++no_local_reads_;
      ++no_local_reads_hit_;
    }
    else
    {
      ++no_remote_reads_;
      ++no_remote_reads_hit_;
    }
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalWrite(WorkRequest *wr)
{
  epicAssert(wr->addr);
  Fence *fence = fences_.at(wr->fd);
  if (!(wr->flag & FENCE))
  {
    fence->lock();
    if (unlikely(IsFenced(fence, wr)))
    {
      epicLog(LOG_DEBUG, "fenced(mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if ((wr->flag & ASYNC) && !(wr->flag & TO_SERVE))
  {
    fence->pending_writes++;
    epicLog(LOG_DEBUG, "Local: one more pending write");
  }
  if (likely(IsLocal(wr->addr)))
  {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    GAddr end = GADD(start, wr->size);
    if (TOBLOCK(end - 1) != start_blk)
    {
      epicLog(LOG_INFO, "read/write split to multiple blocks");
    }
    wr->lock();
    /*
     * we increase it by 1 before we push to the to_serve_local_request queue
     * so we have to decrease by 1 again
     */
    if (wr->flag & TO_SERVE)
    {
      wr->counter--;
    }
    for (GAddr i = start_blk; i < end;)
    {
      epicAssert(
          !(wr->flag & COPY) || ((wr->flag & COPY) && (wr->flag & ASYNC)));

      GAddr nextb = BADD(i, 1);
      void *laddr = ToLocal(i);

      directory.lock(laddr);
      DirEntry *entry = directory.GetEntry(laddr);
      DirState state = directory.GetState(entry);
      if (unlikely(directory.InTransitionState(state)))
      {
        epicLog(LOG_INFO, "directory in transition state when local write %d",
                state);
        // we increase the counter in case
        // we false call Notify()
        wr->counter++;
        wr->is_cache_hit_ = false;
        if (wr->flag & ASYNC)
        {
          if (!wr->IsACopy())
          {
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

      /* add ergeda add */

      DataState Ds = directory.GetDataState(entry);
      GAddr Cur_owner = directory.GetOwner(entry);

      if (Ds != MSI)
      {
        if (Ds == ACCESS_EXCLUSIVE)
        {
          if (WID(Cur_owner) == GetWorkerId())
          { // owner == home_node, 直接在本地写
            GAddr gs = i > start ? i : start;
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            memcpy(ToLocal(gs), ls, len);
          }

          else
          { // owner != home_node, send to owner_node，转发给owner_node去写。
            WorkRequest *lwr = new WorkRequest(*wr);
            lwr->counter = 0;
            Client *cli = GetClient(Cur_owner);
            GAddr gs = i > start ? i : start;
            char buf[BLOCK_SIZE];

            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));     // 要写入的数据，该缓存块对应的数据
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs); // 长度
            memcpy(buf, ls, len);                                        //

            lwr->op = JUST_WRITE;
            lwr->addr = gs;
            lwr->ptr = buf;
            lwr->size = len;

            if (wr->flag & ASYNC)
            { // 防止异步造成栈上的wr丢失
              if (!wr->IsACopy())
              {
                wr->unlock();
                wr = wr->Copy();
                wr->lock();
              }
            }

            lwr->parent = wr;
            lwr->counter = 0;
            wr->counter++; // 存在一个远程请求没搞定
            SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
          }

          directory.unlock(laddr);
          i = nextb;
          continue;
        }

        else if (Ds == READ_MOSTLY)
        {
          // Just_for_test("home_node RmWrite", wr);
          // ProcessrmWrite();
          entry->state = DIR_TO_SHARED; // 等待副本都无效

          GAddr gs = i > start ? i : start;
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          memcpy(ToLocal(gs), ls, len); // 在这里就memcpy一定是对的吗？

          list<GAddr> &shared = directory.GetSList(entry);
          if (shared.size())
          {

            if (wr->flag & ASYNC)
            { // 防止异步造成栈上的wr丢失
              if (!wr->IsACopy())
              {
                wr->unlock();
                wr = wr->Copy();
                wr->lock();
              }
            }

            WorkRequest *lwr = new WorkRequest(*wr);
            lwr->lock();
            lwr->counter = 0;
            lwr->op = RM_FORWARD;
            lwr->addr = i;
            lwr->parent = wr;

            lwr->id = GetWorkPsn();

            lwr->counter = shared.size();
            wr->counter++;

            bool first = true;
            for (auto it = shared.begin(); it != shared.end(); it++)
            {
              Client *cli = GetClient(*it);
              if (first)
              {
                AddToPending(lwr->id, lwr);
                first = false;
              }
              SubmitRequest(cli, lwr);
            }
            lwr->unlock();
          }
          else
          {
            entry->state = DIR_SHARED;
          }

          directory.unlock(laddr);
          i = nextb;
          continue;
        }

        else if (Ds == WRITE_EXCLUSIVE)
        {
          if (WID(Cur_owner) == GetWorkerId())
          { // home_node = owner_node, 本地写

            // entry->state = DIR_TO_SHARED; //等待副本都无效,这底下和read_mostly几乎一致，可以合并到同一个函数。。

            GAddr gs = i > start ? i : start;
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            memcpy(ToLocal(gs), ls, len); // 在这里就memcpy一定是对的吗？异步情况下，似乎不对。

            if (wr->flag & ASYNC)
            { // 防止异步造成栈上的wr丢失
              if (!wr->IsACopy())
              {
                wr->unlock();
                wr = wr->Copy();
                wr->lock();
              }
            }

            Code_invalidate(wr, entry, i);
          }

          else
          { // home_node != owner_node
            WorkRequest *lwr = new WorkRequest(*wr);
            lwr->counter = 0;
            Client *cli = GetClient(Cur_owner);
            GAddr gs = i > start ? i : start;
            char buf[BLOCK_SIZE];

            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));     // 要写入的数据，该缓存块对应的数据
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs); // 长度
            memcpy(buf, ls, len);

            lwr->op = WE_WRITE; // 基本同JUST_WRITE;
            lwr->addr = gs;
            lwr->ptr = buf;
            lwr->size = len;

            if (wr->flag & ASYNC)
            { // 防止异步造成栈上的wr丢失
              if (!wr->IsACopy())
              {
                wr->unlock();
                wr = wr->Copy();
                wr->lock();
              }
            }

            lwr->parent = wr;
            lwr->counter = 0;
            wr->counter++; // 存在一个远程请求没搞定
            SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
          }

          directory.unlock(laddr);
          i = nextb;
          continue;
        }
        
        else if (Ds == WRITE_SHARED)
        {
          /* add wpq add*/
          int subblock_num = directory.GetSubBlockNum(entry, wr->addr);

          int subblock_owner = directory.GetSubBlockOwner(entry, subblock_num);
          GAddr subblock_addr = directory.GetSubBlockAddr(entry, subblock_owner);

          /* add wpq add */
          int offset = wr->addr - TOBLOCK(wr->addr);
          epicLog(LOG_PQ, "Ds = %d, offset = %d,subblock_num(0 start) = %d, subblock_owner(1 start) = %d",
                  Ds, offset, subblock_num, subblock_owner);
          epicLog(LOG_PQ, "3 wr->addr=%lx,start=%lx", wr->addr, start);

          if (subblock_owner == GetWorkerId())
          {
            epicLog(LOG_PQ, "subblock_owner == GetWorkerId()");
            if (i < start)
            {
              epicLog(LOG_PQ, "i<start");
            }
            if (nextb > end)
            {
              epicLog(LOG_PQ, "nextb>end");
            }

            GAddr gs = i > start ? i : start;
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            epicLog(LOG_PQ, "gs=%lx, ls=%lx, len=%d", gs, ls, len);

            memcpy(ToLocal(gs), ls, len);
          }
          else
          {
            epicLog(LOG_PQ, "subblock_owner != GetWorkerId()");
            WorkRequest *lwr = new WorkRequest(*wr);
            lwr->counter = 0;
            Client *sub_owner_client = GetClient(subblock_addr);
            lwr->addr = i;
            lwr->size = BLOCK_SIZE;

            if (wr->flag & ASYNC)
            {
              if (!wr->IsACopy())
              {
                wr->unlock();
                wr = wr->Copy();
                wr->lock();
              }
            }
            lwr->parent = wr;
            DirEntry *Entry = directory.GetEntry((void *)i);
            Entry->ownerlist_subblock[subblock_num] = GetWorkerId();
            int ownerNumber = Entry->ownerNumber;
            epicLog(LOG_PQ, "ownerNumber = %d", ownerNumber);
            GAddr gs = i > start ? i : start;
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            epicLog(LOG_PQ, "gs=%lx, ls=%lx, len=%d", gs, ls, len);

            memcpy(ls, ToLocal(gs), len);

            lwr->op = ChangeSubLog;
            lwr->flagSub1 = subblock_num;
            lwr->flagSub2 = GetWorkerId();
            int temp1 = 0;
            for (temp1 = 1; temp1 <= ownerNumber; temp1++)
            {
              if (temp1 == GetWorkerId())
                continue;
              GAddr client_addr =
                  directory.GetSubBlockAddr(Entry, temp1);
              Client *client = GetClient(client_addr);
              SubmitRequest(client, lwr,
                            ADD_TO_PENDING | REQUEST_SEND);
            }
          }

          directory.unlock(laddr);
          i = nextb;
          continue;
        }
        
        else if (Ds == RC_WRITE_SHARED)
        {

          epicLog(LOG_DEBUG, "RC_WRITE_SHARED,wr->addr=%lx\n", wr->addr);

          GAddr gs = i > start ? i : start;
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          epicLog(LOG_DEBUG, "gs=%lx, ls=%lx, len=%d", gs, ls, len);

          memcpy(ToLocal(gs), ls, len);
          directory.unlock(laddr);

          i = nextb;
          continue;
        }
      }
      /* add ergeda add */

      /*
       * since we cannot guarantee that generating a completion indicates
       * the buf in the remote node has been updated (only means remote HCA received and acked)
       * (ref: http://lists.openfabrics.org/pipermail/general/2007-May/036615.html)
       * so we use Request/Reply mode even for DIR_SHARED invalidations
       * instead of direct WRITE or CAS to invalidate the corresponding cache line in remote node
       */
      if (state == DIR_DIRTY || state == DIR_SHARED)
      {
        if (state == DIR_DIRTY)
        {
          epicLog(LOG_DEBUG, "state == DIR_DIRTY");
        }
        if (state == DIR_SHARED)
        {
          epicLog(LOG_DEBUG, "state == DIR_SHARED");
        }
        list<GAddr> &shared = directory.GetSList(entry);
        WorkRequest *lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        lwr->op = state == DIR_DIRTY ? FETCH_AND_INVALIDATE : INVALIDATE;
        lwr->addr = i;
        lwr->size = BLOCK_SIZE;
        lwr->ptr = laddr;
        wr->is_cache_hit_ = false;
        if (wr->flag & ASYNC)
        {
          if (!wr->IsACopy())
          {
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
            (state == DIR_DIRTY && !directory.IsBlockLocked(entry)) || (state == DIR_SHARED && !directory.IsBlockWLocked(entry)));
        directory.ToToUnShared(entry);
        // we move AddToPending before submit request
        // since it is possible that the reply comes back before we add to the pending list
        // if we AddToPending at last
        AddToPending(lwr->id, lwr);
        for (auto it = shared.begin(); it != shared.end(); it++)
        {
          Client *cli = GetClient(*it);
          epicLog(LOG_DEBUG, "invalidate (%d) cache from worker %d (lwr = %lx)",
                  lwr->op, cli->GetWorkerId(), lwr);
          SubmitRequest(cli, lwr);
          // lwr->counter++;
        }
      }
      else
      {
#ifdef GFUNC_SUPPORT
        if (wr->flag & GFUNC)
        {
          epicAssert(wr->gfunc);
          epicAssert(TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size - 1)));
          epicAssert(i == start_blk);
          void *laddr = ToLocal(wr->addr);
          wr->gfunc(laddr, wr->arg);
        }
        else
        {
#endif
          GAddr gs = i > start ? i : start;
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
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
    if (wr->counter)
    {
      wr->unlock();
      return REMOTE_REQUEST;
    }
    else
    {
      wr->unlock();
    }
  }
  else
  {
    int ret = cache.Write(wr);
    
    if (ret)
    {
      return REMOTE_REQUEST;
    }

    ++no_remote_writes_direct_hit_;
    epicLog(LOG_DEBUG, "++no_remote_writes_direct_hit_", no_remote_writes_direct_hit_.load());
  }
#ifdef MULTITHREAD
  if (wr->flag & ASYNC || wr->flag & TO_SERVE || wr->flag & FENCE)
  {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr))
    {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalRLock(WorkRequest *wr)
{
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));
  // epicAssert(!(wr->flag & FENCE));
  if (!(wr->flag & FENCE))
  {
    Fence *fence = fences_.at(wr->fd);
    fence->lock();
    if (IsFenced(fence, wr))
    {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    }
    else if (fence->pending_writes)
    { // we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from RLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if (IsLocal(wr->addr))
  {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    void *laddr = ToLocal(start_blk);

    wr->lock();
    directory.lock(laddr);
    DirEntry *entry = directory.GetEntry(laddr);
    DirState state = directory.GetState(entry);
    if (directory.InTransitionState(state))
    {
      epicLog(LOG_INFO, "directory in transition state when local read %d",
              state);
      AddToServeLocalRequest(start_blk, wr);
      directory.unlock(laddr);
      wr->unlock();
      return IN_TRANSITION;
    }
    if (state == DIR_DIRTY)
    {
      WorkRequest *lwr = new WorkRequest(*wr);
      lwr->counter = 0;
      GAddr rc = directory.GetSList(entry).front(); // only one worker is updating this line
      Client *cli = GetClient(rc);
      lwr->op = FETCH_AND_SHARED;
      lwr->addr = start_blk;
      lwr->size = BLOCK_SIZE;
      lwr->ptr = laddr;
      lwr->parent = wr;
      lwr->flag |= LOCKED;
      wr->counter++;
      // intermediate state
      epicAssert(state != DIR_TO_SHARED);
      epicAssert(!directory.IsBlockLocked(entry));
      directory.ToToShared(entry, rc);
      SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
    }
    else
    {
      int ret;
      if (entry)
      {
        ret = directory.RLock(entry, ToLocal(wr->addr));
      }
      else
      {
        ret = directory.RLock(ToLocal(wr->addr));
      }
      if (ret)
      { // fail to lock
        epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
        if (wr->flag & TRY_LOCK)
        {
          wr->status = LOCK_FAILED;
        }
        else
        {
          AddToServeLocalRequest(start_blk, wr);
          directory.unlock(laddr);
          wr->unlock();
          return IN_TRANSITION;
        }
      }
    }
    if (wr->counter)
    {
      directory.unlock(laddr);
      wr->unlock();
      return REMOTE_REQUEST;
    }
    else
    {
      directory.unlock(laddr);
      wr->unlock();
    }
  }
  else
  {
    // if there are remote requests
    int ret = cache.RLock(wr);
    if (ret)
    {
      return REMOTE_REQUEST;
    }
  }
#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE)
  {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr))
    {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
  else
  {
    epicAssert(wr->is_cache_hit_);
    if (IsLocal(wr->addr))
    {
      ++no_local_reads_;
      ++no_local_reads_hit_;
    }
    else
    {
      ++no_remote_reads_;
      ++no_remote_reads_hit_;
    }
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalWLock(WorkRequest *wr)
{
  epicAssert(wr->addr);
  epicAssert(!(wr->flag & ASYNC));
  if (!(wr->flag & FENCE))
  {
    Fence *fence = fences_.at(wr->fd);
    fence->lock();
    if (IsFenced(fence, wr))
    {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    }
    else if (fence->pending_writes)
    { // we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from WLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }
  if (IsLocal(wr->addr))
  {
    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    void *laddr = ToLocal(start_blk);

    wr->lock();
    directory.lock(laddr);
    DirEntry *entry = directory.GetEntry(laddr);
    DirState state = directory.GetState(entry);
    if (directory.InTransitionState(state))
    {
      epicLog(LOG_INFO, "directory in transition state when local write %d",
              state);
      AddToServeLocalRequest(start_blk, wr);
      directory.unlock(laddr);
      wr->unlock();
      return IN_TRANSITION;
    }
    if (DIR_DIRTY == state || DIR_SHARED == state)
    {
      list<GAddr> &shared = directory.GetSList(entry);
      WorkRequest *lwr = new WorkRequest(*wr);
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
          (state == DIR_DIRTY && !directory.IsBlockLocked(entry)) || (state == DIR_SHARED && !directory.IsBlockWLocked(entry)));
      directory.ToToUnShared(entry);
      AddToPending(lwr->id, lwr);
      for (auto it = shared.begin(); it != shared.end(); it++)
      {
        Client *cli = GetClient(*it);
        epicLog(
            LOG_DEBUG,
            "invalidate (%d) cache from worker %d, state = %d, lwr->counter = %d",
            lwr->op, cli->GetWorkerId(), state, lwr->counter.load());
        SubmitRequest(cli, lwr);
        // lwr->counter++;
      }
    }
    else if (DIR_UNSHARED == state)
    {
      int ret;
      if (entry)
      {
        ret = directory.WLock(entry, ToLocal(wr->addr));
      }
      else
      {
        ret = directory.WLock(ToLocal(wr->addr));
      }
      if (ret)
      { // failed to lock
        epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
        if (wr->flag & TRY_LOCK)
        {
          wr->status = LOCK_FAILED;
        }
        else
        {
          AddToServeLocalRequest(start_blk, wr);
          directory.unlock(laddr);
          wr->unlock();
          return IN_TRANSITION;
        }
      }
    }
    if (wr->counter)
    {
      directory.unlock(laddr);
      wr->unlock();
      return REMOTE_REQUEST;
    }
    else
    {
      directory.unlock(laddr);
      wr->unlock();
    }
  }
  else
  {
    int ret = cache.WLock(wr);
    if (ret)
    {
      return REMOTE_REQUEST;
    }
  }
#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE)
  {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr))
    {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
  else
  {
    epicAssert(wr->is_cache_hit_);
    if (IsLocal(wr->addr))
    {
      ++no_local_writes_;
      ++no_local_writes_hit_;
    }
    else
    {
      ++no_remote_writes_;
      ++no_remote_writes_hit_;
    }
  }
#endif
  return SUCCESS;
}

int Worker::ProcessLocalUnLock(WorkRequest *wr)
{
  if (!(wr->flag & FENCE))
  {
    Fence *fence = fences_.at(wr->fd);
    fence->lock();
    if (IsFenced(fence, wr))
    {
      AddToFence(fence, wr);
      fence->unlock();
      epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
              fence->mfenced, fence->sfenced, wr->op);
      return FENCE_PENDING;
    }
    else if (fence->pending_writes)
    { // we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced from UNLOCK!");
      AddToFence(fence, wr);
      fence->unlock();
      return FENCE_PENDING;
    }
    fence->unlock();
  }

  if (IsLocal(wr->addr))
  {
    GAddr start_blk = TOBLOCK(wr->addr);
    void *laddr = ToLocal(start_blk);
    directory.lock(laddr);
    directory.UnLock(ToLocal(wr->addr));
    directory.unlock(laddr);
  }
  else
  {
    cache.UnLock(wr->addr);
  }
  ProcessToServeRequest(wr);
#ifdef MULTITHREAD
  if (wr->flag & TO_SERVE || wr->flag & FENCE)
  {
#endif
    /*
     * notify the app thread directly
     * this can only happen when the request can be fulfilled locally
     * or we don't need to wait for reply from remote node
     */
    if (Notify(wr))
    {
      epicLog(LOG_WARNING, "cannot wake up the app thread");
    }
#ifdef MULTITHREAD
  }
#endif
  return SUCCESS;
}
/* add ergeda add */
void Worker::CreateCache(WorkRequest *wr, DataState Dstate)
{
  // Just_for_test("CreateCache", wr);
  GAddr start = wr->addr;
  GAddr start_blk = TOBLOCK(start);
  GAddr end = GADD(start, wr->size);

  for (GAddr i = start_blk; i < end;)
  {
    GAddr nextb = BADD(i, 1);
    cache.lock(i);
    CacheLine *cline = nullptr;
    cline = cache.SetCLine(i);
    cache.unlock(i);
    i = nextb;
  }
}

void Worker::Code_invalidate(WorkRequest *wr, DirEntry *entry, GAddr blk)
{
  list<GAddr> &shared = directory.GetSList(entry);
  if (shared.size())
  {
    entry->state = DIR_TO_SHARED;

    WorkRequest *lwr = new WorkRequest(*wr);
    lwr->lock();
    lwr->counter = 0;

    if (entry->Dstate == READ_MOSTLY)
      lwr->op = RM_FORWARD;
    else if (entry->Dstate == WRITE_EXCLUSIVE)
      lwr->op = WE_INV;

    lwr->addr = blk;
    lwr->parent = wr;

    lwr->id = GetWorkPsn();

    lwr->counter = shared.size();
    wr->counter++;

    bool first = true;
    for (auto it = shared.begin(); it != shared.end(); it++)
    {
      Client *cli = GetClient(*it);
      if (first)
      {
        AddToPending(lwr->id, lwr);
        first = false;
      }
      SubmitRequest(cli, lwr);
    }
    lwr->unlock();
  }
}
/* add ergeda add */