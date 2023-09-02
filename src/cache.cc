// Copyright (c) 2018 The GAM Authors

#include <cstring>
#include "cache.h"
#include "client.h"
#include "worker.h"
#include "slabs.h"
#include "kernel.h"

int Cache::ReadWrite(WorkRequest *wr)
{
  epicLog(LOG_DEBUG, "op=%d, addr=%lx, size=%d", wr->op, wr->addr, wr->size);
  epicLog(LOG_DEBUG, "here wid=%d", wr->wid);
#ifdef NOCACHE
  epicLog(LOG_WARNING, "shouldn't come here");
  return 0;
#endif
  epicAssert(READ == wr->op || WRITE == wr->op);
  int newcline = 0;
  GAddr start_blk = TOBLOCK(wr->addr);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr end_blk = TOBLOCK(end - 1);
  if (end_blk != start_blk)
  {
    epicLog(LOG_INFO, "read/write split to multiple blocks");
  }
  Client *cli = worker->GetClient(wr->addr);
  GAddr start = wr->addr;

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
    epicAssert(!(wr->flag & COPY) || ((wr->flag & COPY) && (wr->flag & ASYNC)));

    GAddr nextb = BADD(i, 1);
    /* add ergeda add */
    bool breakflag = 0; //循环内还有循环枚举子块，用于判断是否需要跳出循环(好像并没用)
    worker->directory.lock((void *)i);
    DirEntry *Entry = worker->directory.GetEntry((void *)i);
    if (Entry == nullptr)
    { // no metadata in the first time;
      // worker->Just_for_test("get Directory", wr);
      worker->directory.CreateEntry((void *)i);
      Entry = worker->directory.GetEntry((void *)i);
      worker->directory.ToToDirty(Entry, i);
      WorkRequest *lwr = new WorkRequest(*wr);

      lwr->counter = 0;
      lwr->op = READ_TYPE;
      lwr->addr = i;

      if (wr->flag & ASYNC)
      {
        if (!wr->IsACopy())
        {
          wr->unlock();
          wr = wr->Copy();
          wr->lock();
        }
      }

      wr->counter++;
      worker->AddToServeLocalRequest(i, wr);
      worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
      worker->directory.unlock((void *)i);
      wr->unlock();
      return IN_TRANSITION;
    }

    else
    {
      if (unlikely(worker->directory.InTransitionState(worker->directory.GetState(Entry))))
      { // have not got datastate and owner
        epicLog(LOG_INFO, "in transition state while cache read/write(%d)", wr->op);
        // we increase the counter in case
        // we false call Notify()
        wr->counter++;
        wr->unlock();
        if (wr->flag & ASYNC)
        {
          if (!wr->IsACopy())
          {
            // wr->unlock();
            wr = wr->Copy();
            // wr->lock();
          }
        }
        // worker->to_serve_local_requests[i].push(wr);
        worker->AddToServeLocalRequest(i, wr);
        worker->directory.unlock((void *)i);
        // wr->unlock();
        return 1;
      }
    }

    DataState Cur_Dstate = worker->directory.GetDataState(Entry);
    GAddr Cur_owner = worker->directory.GetOwner(Entry);

    if (Cur_Dstate != WRITE_EXCLUSIVE)
      worker->directory.unlock((void *)i);
    if (Cur_Dstate == DataState::ACCESS_EXCLUSIVE)
    {
      if (worker->GetWorkerId() == WID(Cur_owner))
      { // local_node == owner_node，直接读/写
        lock(i);
        CacheLine *cline = nullptr;
        cline = GetCLine(i);

        GAddr gs = i > start ? i : start;
        epicAssert(GMINUS(nextb, gs) > 0);
        void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
        void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        if (wr->op == READ)
          memcpy(ls, cs, len);
        else if (wr->op == WRITE)
          memcpy(cs, ls, len);

        unlock(i);
        i = nextb;
        continue;
      }

      else
      { // local_node != owner_node，需要转发，把local_request那里的复制过来基本就行
        // worker->Just_for_test("forward_read/write to Owner_node", wr);
        WorkRequest *lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        Client *Owner_cli = worker->GetClient(Cur_owner);

        lwr->op = JUST_READ;
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

        wr->is_cache_hit_ = false;

        lwr->parent = wr;
        wr->counter++;

        if (wr->op == WRITE)
        {

          GAddr gs = i > start ? i : start;
          char buf[BLOCK_SIZE];

          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          memcpy(buf, ls, len);

          lwr->op = JUST_WRITE;
          lwr->addr = gs;
          lwr->ptr = buf;
          lwr->size = len;

          worker->SubmitRequest(Owner_cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
          // forget to delete wr
        }

        else
        {
          lock(i);
          CacheLine *cline = nullptr;
          cline = SetCLine(i);
          unlock(i);
          lwr->ptr = cline->line;

          worker->SubmitRequest(Owner_cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
        }

        i = nextb;
        continue;
      }
    }

    else if (Cur_Dstate == DataState::READ_MOSTLY)
    {
      if (wr->op == READ)
      { // read
        // worker->Just_for_test("Rm_read_cache", wr);
        lock(i);
        CacheLine *cline = nullptr;
        cline = GetCLine(i);
        if (cline == nullptr)
        { // first time read,需要去home_node要数据，并更新shared_list
          cline = SetCLine(i);
          cline->state = CACHE_TO_SHARED; // 等拿到数据再转
          WorkRequest *lwr = new WorkRequest(*wr);
          lwr->counter = 0;
          lwr->ptr = cline->line;
          lwr->addr = i;
          lwr->size = BLOCK_SIZE;
          lwr->counter = 0;
          lwr->parent = wr;
          lwr->op = RM_READ;
          wr->counter++;

          worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
          unlock(i);
          i = nextb;
          continue;
        }
        else
        { // 已经有数据，直接读取就行
          CacheState state = cline->state;
          if (unlikely(InTransitionState(state)))
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
            worker->AddToServeLocalRequest(i, wr);
            unlock(i);
            return 1;
          }

          GAddr gs = i > start ? i : start;
          epicAssert(GMINUS(nextb, gs) > 0);
          void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          memcpy(ls, cs, len);
          unlock(i);
          i = nextb;
          continue;
        }
      }
      else
      { // op = WRITE
        // worker->Just_for_test("Rm_write_cache", wr);
        lock(i);
        CacheLine *cline = nullptr;
        cline = GetCLine(i);

        if (cline != nullptr)
        {
          CacheState state = cline->state;
          if (unlikely(InTransitionState(state)))
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
            worker->AddToServeLocalRequest(i, wr);
            unlock(i);
            return 1;
          }
        }

        WorkRequest *lwr = new WorkRequest(*wr);

        if (wr->flag & ASYNC)
        {
          if (!wr->IsACopy())
          {
            wr->unlock();
            wr = wr->Copy();
            wr->lock();
          }
        }

        if (cline == nullptr)
        { // 第一次对该缓存块操作
          lwr->flag |= Add_list;
          cline = SetCLine(i);
          cline->state = CACHE_TO_INVALID;
        }
        cline->state = CACHE_TO_INVALID; // TO_INVALID表示该节点作为发送写请求的节点

        GAddr gs = i > start ? i : start;
        char buf[BLOCK_SIZE];

        void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        memcpy(buf, ls, len);

        lwr->op = RM_WRITE;
        lwr->addr = gs;
        lwr->ptr = buf;
        lwr->size = len;
        lwr->next = (WorkRequest *)(cline->line);
        lwr->parent = wr;
        wr->counter++;

        worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);

        unlock(i);
        i = nextb;
        continue;
      }
    }

    else if (Cur_Dstate == DataState::WRITE_EXCLUSIVE)
    {
      CacheLine *cline = nullptr;
      if (worker->GetWorkerId() == WID(Cur_owner))
      { // local_node == owner_node，直接读/写
        lock(i);

        cline = GetCLine(i);

        GAddr gs = i > start ? i : start;
        epicAssert(GMINUS(nextb, gs) > 0);
        void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
        void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        if (wr->op == READ)
          memcpy(ls, cs, len);
        else if (wr->op == WRITE)
        {
          memcpy(cs, ls, len);
          if (wr->flag & ASYNC)
          { // 防止异步造成栈上的wr丢失
            if (!wr->IsACopy())
            {
              wr->unlock();
              wr = wr->Copy();
              wr->lock();
            }
          }
          worker->Code_invalidate(wr, Entry, i);
        }

        worker->directory.unlock((void *)i);
        unlock(i);
        i = nextb;
        continue;
      }

      else
      { // 在别的节点上
        worker->directory.unlock((void *)i);
        lock(i);
        WorkRequest *lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        Client *Cur_cli = worker->GetClient(Cur_owner);
        GAddr gs = i > start ? i : start;
        char buf[BLOCK_SIZE];
        if (wr->op == WRITE)
        {
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));     // 要写入的数据，该缓存块对应的数据
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs); // 长度
          memcpy(buf, ls, len);

          lwr->op = WE_WRITE; // 基本同JUST_WRITE;
          lwr->addr = gs;
          lwr->ptr = buf;
          lwr->size = len;
        }

        else if (wr->op == READ)
        {
          if (cline = GetCLine(i))
          { // 有缓存
            CacheState state = cline->state;
            if (unlikely(InTransitionState(state)))
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
              worker->AddToServeLocalRequest(i, wr);
              unlock(i);
              return 1;
            }

            void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
            void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
            int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
            memcpy(ls, cs, len);

            unlock(i);
            i = nextb;
            continue;
          }
          else
          {
            cline = SetCLine(i);
          }
          lwr->op = WE_READ; // 基本同JUST_WRITE;
          lwr->addr = i;
          lwr->size = BLOCK_SIZE;
          lwr->ptr = cline->line;
        }

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
        worker->SubmitRequest(Cur_cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
        unlock(i);
        i = nextb;
        continue;
      }
    }
    // worker->Just_for_test("cache.cc", wr);
    /* add ergeda add */
/* add xmx add */
#ifdef SUB_BLOCK
    else if (Cur_Dstate == DataState::WRITE_SHARED) {
      int CurSize = Entry->MySize; //拿到现在的目录大小
      nextb = i + CurSize; //修改nextb的大小
      if (nextb <= start) { //还没到
        worker->directory.unlock((void*)i);
        i = nextb;
        continue;
      }

      lock(i);
      CacheLine* cline = nullptr;
      cline = GetSubCline(i);

      CacheState state = CACHE_INVALID;
      if (cline) state = cline->state;
      if (state != CACHE_INVALID) { //原本存在cache
        if (state == CACHE_TO_INVALID && READ == wr->op) {
          epicLog(LOG_INFO, "cache is going to be invalid, but still usable for read op = %d", wr->op);
          GAddr gs = i > start ? i : start;
          epicAssert(GMINUS(nextb, gs) > 0);
          void* cs = (void*) ((ptr_t) cline->line + GMINUS(gs, i));
          void* ls = (void*) ((ptr_t) wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          memcpy(ls, cs, len);
          unlock(i);
          worker->directory.unlock((void*)i);
          i = nextb;
          continue;
        }

        //special processing when cache is in process of to_to_dirty
        //for WRITE, cannot allow since it may dirty the cacheline before
        //it finished transmission
        if (state == CACHE_TO_DIRTY && READ == wr->op && IsBlockLocked(cline)) {
          epicAssert(!IsBlockWLocked(cline));
          epicLog(
              LOG_INFO, "cache is going from shared to dirty, but still usable for read op = %d", wr->op);
          GAddr gs = i > start ? i : start;
          epicAssert(GMINUS(nextb, gs) > 0);
          void* cs = (void*) ((ptr_t) cline->line + GMINUS(gs, i));
          void* ls = (void*) ((ptr_t) wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          memcpy(ls, cs, len);
          unlock(i);
          worker->directory.unlock((void*)i);
          i = nextb;
          continue;
        }

        if (unlikely(InTransitionState(state))) {
          epicLog(LOG_INFO, "in transition state while cache read/write(%d)", wr->op);
          //we increase the counter in case
          //we false call Notify()
          wr->counter++;
          wr->unlock();
          if (wr->flag & ASYNC) {
            if (!wr->IsACopy()) {
              //wr->unlock();
              wr = wr->Copy();
              //wr->lock();
            }
          }
          //worker->to_serve_local_requests[i].push(wr);
          worker->AddToServeLocalRequest(i, wr);
          unlock(i);
          worker->directory.unlock((void*)i);
          //wr->unlock();
          return 1;
        }
        epicAssert(state == CACHE_SHARED || state == CACHE_DIRTY);

        GAddr gs = i > start ? i : start;
        epicAssert(GMINUS(nextb, gs) > 0);
        void* cs = (void*) ((ptr_t) cline->line + GMINUS(gs, i));
        void* ls = (void*) ((ptr_t) wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        //this line is either shared in shared-only or dirty mode
        //we can copy the data immediately since it will not be over-written by remote node
        //and also allow following read ops get the latest data
        if (READ == wr->op) {
          memcpy(ls, cs, len);
  /*
  #ifdef USE_LRU
          UnLinkLRU(cline);
          LinkLRU(cline);
  #endif
  */
        } else if (WRITE == wr->op) {
          if (state != CACHE_DIRTY) {
            wr->is_cache_hit_ = false;
            WorkRequest* lwr = new WorkRequest(*wr);
            lwr->counter = 0;
            lwr->op = WRITE_PERMISSION_ONLY;
            lwr->flag |= CACHED;
            lwr->addr = i;
            lwr->size = CurSize; //different
            lwr->ptr = cline->line;
#ifdef DYNAMIC
            lwr->Version = Entry->MetaVersion;
#endif
            if (wr->flag & ASYNC) {
              if (!wr->IsACopy()) {
                wr->unlock();
                wr = wr->Copy();
                wr->lock();
              }
            }
            lwr->parent = wr;
            wr->counter++;
            //to intermediate state
            epicAssert(state != CACHE_TO_DIRTY);
            ToToDirty(cline);
/*
  #ifdef USE_LRU
            UnLinkLRU(cline);
  #endif
*/
            worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
          } else {
              epicAssert(len);
              worker->logWrite(wr->addr, len, ls);
              memcpy(cs, ls, len);
  /*
  #ifdef USE_LRU
            UnLinkLRU(cline);
            LinkLRU(cline);
  #endif
  */
          }
        } else {
          epicLog(LOG_WARNING, "unknown op in cache operations %d", wr->op);
          epicAssert(false);
        }
      } else { //不存在子块
        //worker->Just_for_test("cache invalid", wr);
        WorkRequest* lwr = new WorkRequest(*wr);
  
        //newcline++;
        cline = SetSubline(i, CurSize);
        lwr->counter = 0;
        lwr->flag |= CACHED;
        lwr->addr = i;
        lwr->size = CurSize;
        lwr->ptr = cline->line;
        wr->is_cache_hit_ = false;
        if (wr->flag & ASYNC) {
          if (!wr->IsACopy()) {
            wr->unlock();
            wr = wr->Copy();
            wr->lock();
          }
        }
        lwr->parent = wr;
#ifdef DYNAMIC
        lwr->Version = Entry->MetaVersion;
#endif
        wr->counter++;
        //to intermediate state
        if (READ == wr->op) {
          epicAssert(cline->state != CACHE_TO_SHARED);
          ToToShared(cline);
        } else {  //WRITE
          epicAssert(cline->state != CACHE_TO_DIRTY);
          ToToDirty(cline);
        }
        worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
      }
      unlock(i);
      worker->directory.unlock((void*)i);
      i = nextb;
      continue;
    }
#endif
#ifdef B_I
    else if (Cur_Dstate == DataState::BI) {

      worker->directory.unlock((void*)i);

      GAddr gs = i > start ? i : start;
      epicAssert(GMINUS(nextb, gs) > 0);
      void* ls = (void*) ((ptr_t) wr->ptr + GMINUS(gs, start));
      int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);

      if (WRITE == wr->op) {
        worker->write_miss += 1;
        WorkRequest* lwr = new WorkRequest(*wr);
        if (wr->flag & ASYNC) {
          if (!wr->IsACopy()) {
            wr->unlock();
            wr = wr->Copy();
            wr->lock();
          }
        }
        wr->is_cache_hit_ = false;
        wr->counter++;

        char buf[BLOCK_SIZE];
        
        memcpy(buf, ls, len);

        lwr->parent = wr;
        lwr->counter = 0;
        lwr->op = BI_WRITE;
        lwr->addr = gs;
        lwr->ptr = buf;
        lwr->size = len;

        worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
        
        i = nextb;
        continue;
      }

      lock(i);
      CacheLine * cline = nullptr;
      cline = GetCLine(i);
      CacheState Curs = CACHE_INVALID;
      if (cline) Curs = cline->state;
      //below are read option
      if (Curs != CACHE_INVALID) { //TODO: && GMINUS(get_time(), cline->timestamp) < max_timediff
        worker->read_hit += 1;
        void* cs = (void*) ((ptr_t) cline->line + GMINUS(gs, i));
        MyAssert(wr->op == READ);
        // 这里是否需要添加辅助判断：若过期时间过长，则也将其无效。
        memcpy(ls, cs, len);
      }
      else { //no cache
        worker->read_miss += 1;
        if (cline == nullptr) cline = SetCLine(i);
        cline->state = CACHE_INVALID;
        WorkRequest* lwr = new WorkRequest(*wr);
        wr->is_cache_hit_ = false;
        wr->counter++;
        lwr->parent = wr;
        lwr->counter = 0;
        lwr->op = BI_READ;
        lwr->addr = i;
        lwr->ptr = cline->line;
#ifdef SUB_BLOCK
        lwr->size = Entry->MySize;
#else
        lwr->size = BLOCK_SIZE;
#endif
        worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
      }
      unlock(i);
      i = nextb;
      continue;
    }
#endif
    else if (Cur_Dstate == DataState::RC_WRITE_SHARED)
    {
      epicLog(LOG_DEBUG, "RC_WRITE_SHARED, GetWorkerId=%d\n", worker->GetWorkerId());

      int offset = wr->addr - TOBLOCK(wr->addr);

      CacheLine *cline = nullptr;
      lock(i);
      if (cline = GetCLine(i))
      {
        epicLog(LOG_DEBUG, "GetCLine SUCCESS,i=%lx\n", i);
      }
      else
      {
        cline = SetCLine(i);
        epicLog(LOG_DEBUG, "GetCLine Failure,i=%lx,cline->addr=%lx,cline->line=%lx\n,workid=%d",
                i, cline->addr, cline->line, worker->GetWorkerId());
      }

      GAddr gs = i > start ? i : start;
      epicAssert(GMINUS(nextb, gs) > 0);
      void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
      void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
      int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
      if (wr->op == READ)
      {
        memcpy(ls, cs, len);
      }

      else if (wr->op == WRITE)
      {
        memcpy(cs, ls, len);
      }

      epicLog(LOG_DEBUG, "wr->op=%d,offset=%d,cs= %lx, ls = %lx\n",
              wr->op, offset, cs, ls);

      unlock(i);
      i = nextb;
      epicLog(LOG_DEBUG, "wr->flush_id.load()=%d\n", wr->flush_id.load());

      if (wr->flush_id.load() > 0 && wr->flush_id.load() < 10 && wr->op == WRITE)
      {
        
        // printf("call AddToFlushList, flush_id=%d, addr=%lx\n", wr->flush_id.load(), wr->addr);
        worker->AddToFlushList(wr->flush_id.load(), wr->addr);
      }

      continue;
    }
    lock(i);
    CacheLine *cline = nullptr;
    if ((cline = GetCLine(i)))
    {
      worker->no_cache_exist_++;
      CacheState state = cline->state;
      // FIXME: may violate the ordering guarantee of single thread
      // special processing when cache is in process of eviction
      // for WRITE, cannot allow since it may dirty the cacheline before
      // it finished transmission
      if (state == CACHE_TO_INVALID && READ == wr->op)
      {
#ifdef B_I
        worker->read_hit += 1;
#endif
        worker->no_cache_state_toinvalid_++;
        epicLog(LOG_INFO, "cache is going to be invalid, but still usable for read op = %d", wr->op);
        GAddr gs = i > start ? i : start;
        epicAssert(GMINUS(nextb, gs) > 0);
        void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
        void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        memcpy(ls, cs, len);
        unlock(i);
        i = nextb;
        continue;
      }

      // special processing when cache is in process of to_to_dirty
      // for WRITE, cannot allow since it may dirty the cacheline before
      // it finished transmission
      if (state == CACHE_TO_DIRTY && READ == wr->op && IsBlockLocked(cline))
      {
#ifdef B_I
        worker->read_hit += 1;
#endif
        worker->no_cache_state_todirty_++;
        epicAssert(!IsBlockWLocked(cline));
        epicLog(
            LOG_INFO, "cache is going from shared to dirty, but still usable for read op = %d", wr->op);
        GAddr gs = i > start ? i : start;
        epicAssert(GMINUS(nextb, gs) > 0);
        void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
        void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
        int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
        memcpy(ls, cs, len);
        unlock(i);
        i = nextb;
        continue;
      }

      if (unlikely(InTransitionState(state)))
      {
        if (wr->op == READ)
        {
          worker->no_cache_state_InTransition_read++;
        }
        else if (wr->op == WRITE)
        {
          worker->no_cache_state_InTransition_write++;
        }

        if (state == CACHE_TO_DIRTY)
        {
          epicLog(LOG_DEBUG, "state = CACHE_TO_DIRTY");
        }
        else if (state == CACHE_TO_SHARED)
        {
          epicLog(LOG_DEBUG, "CACHE_TO_SHARED");
        }
        else if (state == CACHE_TO_INVALID)
        {
          epicLog(LOG_DEBUG, "state = CACHE_TO_INVALID");
        }
        epicLog(LOG_INFO, "in transition state while cache read/write(%d)", wr->op);
        // we increase the counter in case
        // we false call Notify()
        wr->counter++;
        wr->unlock();
        if (wr->flag & ASYNC)
        {
          if (!wr->IsACopy())
          {
            // wr->unlock();
            wr = wr->Copy();
            // wr->lock();
          }
        }
        // worker->to_serve_local_requests[i].push(wr);
        worker->AddToServeLocalRequest(i, wr);
        unlock(i);
        // wr->unlock();
        return 1;
      }
      epicAssert(state == CACHE_SHARED || state == CACHE_DIRTY);

      GAddr gs = i > start ? i : start;
      epicAssert(GMINUS(nextb, gs) > 0);
      void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
      void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
      int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
      // this line is either shared in shared-only or dirty mode
      // we can copy the data immediately since it will not be over-written by remote node
      // and also allow following read ops get the latest data
      if (READ == wr->op)
      {
#ifdef SELECTIVE_CACHING
        cline->nread++;
        nread++;
#endif
        worker->no_cache_read_hit_++;
        memcpy(ls, cs, len);
#ifdef B_I
        worker->read_hit += 1;
#endif
#ifdef USE_LRU
        UnLinkLRU(cline);
        LinkLRU(cline);
#endif
      }
      else if (WRITE == wr->op)
      {
#ifdef SELECTIVE_CACHING
        cline->nwrite++;
        nwrite++;
#endif
        if (state != CACHE_DIRTY)
        {
#ifdef B_I
        worker->write_miss += 1;
#endif
          worker->no_cache_state_shared_++;
          epicAssert(state == CACHE_SHARED);
          //        we comment below deadlock handle since we add it the worker deadlock case 3
          //					/*
          //					 * below is used to avoid deadlock
          //					 * when we are in transition state (want to get ownership) and read locked,
          //					 * home node wants to invalidate it (home becomes in transition state).
          //					 * both will block and the deadlock solution in worker.cc:1775 is not enough,
          //					 * because read lock will block it forever
          //					 * if the thread holding the lock wants to read the data (will be blocked
          //					 * since it is in transition state)
          //					 */
          //					if(IsBlockLocked(i)) {
          //						epicAssert(!IsBlockWLocked(i));
          //						epicLog(LOG_INFO, "read locked while cache write(%d)", wr->op);
          //						//we increase the counter in case
          //						//we false call Notify()
          //						wr->counter++;
          //						unlock(i);
          //						wr->unlock();
          //						//worker->to_serve_local_requests[i].push(wr);
          //						worker->AddToServeLocalRequest(i, wr);
          //						return 1;
          //					}

#ifdef SELECTIVE_CACHING
          wr->flag &= ~NOT_CACHE;
#endif
          wr->is_cache_hit_ = false;
          WorkRequest *lwr = new WorkRequest(*wr);
          lwr->counter = 0;
          lwr->op = WRITE_PERMISSION_ONLY; // diff
          lwr->flag |= CACHED;
          lwr->addr = i;
          lwr->size = BLOCK_SIZE;
          lwr->ptr = cline->line; // diff
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
          // to intermediate state
          epicAssert(state != CACHE_TO_DIRTY);
          ToToDirty(cline);
          // worker->AddToPending(lwr->id, lwr);

#ifdef USE_LRU
          // we unlink the cache to avoid it is evicted before
          // the reply comes back
          // in which case it will cause error
          UnLinkLRU(cline);
#endif

          // short-circuit copy
          // FIXME: advanced copy is not necessary
          // since the cache line is in transition state,
          // which will block other ops to proceed
          epicAssert(len);
#ifdef GFUNC_SUPPORT
          if (!(wr->flag & GFUNC))
          {
            memcpy(cs, ls, len);
          }
#else
          memcpy(cs, ls, len);
#endif

          // put submit request at last in case reply comes before we process afterwards works
          worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
        }
        else
        {
          worker->no_cache_state_dirty_++;
#ifdef GFUNC_SUPPORT
          if (wr->flag & GFUNC)
          {
            epicAssert(wr->gfunc);
            epicAssert(
                TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size - 1)));
            epicAssert(i == start_blk);
            void *laddr = cs;
            wr->gfunc(laddr, wr->arg);
          }
          else
          {
#endif
            epicAssert(len);
            worker->logWrite(wr->addr, len, ls);
            memcpy(cs, ls, len);
#ifdef B_I
            worker->write_hit += 1;
#endif
#ifdef GFUNC_SUPPORT
          }
#endif

#ifdef USE_LRU
          UnLinkLRU(cline);
          LinkLRU(cline);
#endif
        }
      }
      else
      {
        epicLog(LOG_WARNING, "unknown op in cache operations %d", wr->op);
        epicAssert(false);
      }
    }
    else
    {
      worker->no_cache_miss_++;
      epicLog(LOG_DEBUG, "cache miss,optype = %d", wr->op);
      // worker->Just_for_test("cache invalid", wr);
      WorkRequest *lwr = new WorkRequest(*wr);
#ifdef SELECTIVE_CACHING
      if (!cline)
      {
        newcline++;
        cline = SetCLine(i);
      }
      else
      {
        if (WRITE == wr->op)
        {
          InitCacheCLine(cline, true);
        }
        else
        {
          InitCacheCLine(cline);
        }
      }
      if (!IsCachable(cline, lwr))
      {
        lwr->flag |= NOT_CACHE;
        cline->state = CACHE_NOT_CACHE;
      }
      else
      {
        InitCacheCLineIfNeeded(cline);
      }
#else
      newcline++;
      cline = SetCLine(i);
#endif
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
      // to intermediate state
      if (READ == wr->op)
      {
#ifdef B_I
        worker->read_miss += 1;
#endif
        epicAssert(cline->state != CACHE_TO_SHARED);
        ToToShared(cline);
#ifdef SELECTIVE_CACHING
        if (lwr->flag & NOT_CACHE)
        {
          GAddr gs = i > start ? i : start;
          void *cs = (void *)((ptr_t)cline->line + GMINUS(gs, i));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          epicAssert(len > 0 && len <= BLOCK_SIZE);
          lwr->addr = gs;
          lwr->ptr = cs;
          lwr->size = len;
        }
#endif
      }
      else
      { // WRITE
#ifdef B_I
        worker->write_miss += 1;
#endif
#ifdef SELECTIVE_CACHING
        if (lwr->flag & NOT_CACHE)
        {
          GAddr gs = i > start ? i : start;
          void *ls = (void *)((ptr_t)wr->ptr + GMINUS(gs, start));
          int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
          epicAssert(len > 0 && len <= BLOCK_SIZE);
          lwr->addr = gs;
          lwr->ptr = ls;
          lwr->size = len;
        }
#endif
        epicAssert(cline->state != CACHE_TO_DIRTY);
        ToToDirty(cline);
      }
      worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
    }
    unlock(i);
    i = nextb;
  }
  int ret = wr->counter;
  wr->unlock();
#ifdef USE_LRU
  if (newcline)
  {
    // if (newcline && !(wr->flag & ASYNC)) {
    Evict(newcline);
  }
#endif
  return ret;
}

int Cache::Lock(WorkRequest *wr)
{
#ifdef NOCACHE
  epicLog(LOG_WARNING, "shouldn't come here");
  return 0;
#endif
  epicAssert(RLOCK == wr->op || WLOCK == wr->op);
  int newcline = 0;
  GAddr i = TOBLOCK(wr->addr);
  Client *cli = worker->GetClient(wr->addr);
  GAddr start = wr->addr;

  wr->lock();
  lock(i);
  CacheLine *cline = nullptr;
#ifdef SELECTIVE_CACHING
  if ((cline = GetCLine(i)) && cline->state != CACHE_NOT_CACHE)
  {
#else
  if ((cline = GetCLine(i)))
  {
#endif
    CacheState state = cline->state;
    // since transition state in cache is caused by local requests
    // we allow to advance without checking
    if (InTransitionState(state))
    {
      epicLog(LOG_INFO, "in transition state while cache read/write(%d)", wr->op);
      wr->is_cache_hit_ = false;
      worker->AddToServeLocalRequest(i, wr);
      unlock(i);
      wr->unlock();
      return 1;
    }
    epicAssert(state == CACHE_SHARED || state == CACHE_DIRTY);
    if (RLOCK == wr->op)
    {
#ifdef SELECTIVE_CACHING
      if (!(wr->flag & TO_SERVE))
      {
        cline->nread++;
        nread++;
      }
#endif
      if (RLock(cline, wr->addr))
      { // failed to lock
        epicLog(LOG_INFO, "cannot shared lock addr %lx, will try later", wr->addr);

        wr->is_cache_hit_ = false;

        if (wr->flag & TRY_LOCK)
        {
          wr->status = LOCK_FAILED;
          unlock(i);
          wr->unlock();
          return SUCCESS;
        }
        else
        {
          worker->AddToServeLocalRequest(i, wr);
          unlock(i);
          wr->unlock();
          return IN_TRANSITION;
        }
      }
#ifdef USE_LRU
      UnLinkLRU(cline);
      LinkLRU(cline);
#endif
    }
    else if (WLOCK == wr->op)
    {
#ifdef SELECTIVE_CACHING
      if (!(wr->flag & TO_SERVE))
      {
        cline->nwrite++;
        nwrite++;
      }
#endif
      if (state != CACHE_DIRTY)
      {
        epicAssert(state == CACHE_SHARED);
        wr->is_cache_hit_ = false;

        //        we comment below deadlock handle since we add it the worker deadlock case 3
        //				/*
        //				 * below is used to avoid deadlock
        //				 * when we are in transition state (want to get ownership) and read locked,
        //				 * home node wants to invalidate it (home becomes in transition state).
        //				 * both will block and the deadlock solution in worker.cc:deadlock case 1 is not enough,
        //				 * because read lock will block it forever
        //				 * if the thread holding the lock wants to read the data (will be blocked
        //				 * since it is in transition state)
        //				 */
        //				if(IsBlockLocked(i)) {
        //					epicAssert(!IsBlockWLocked(i));
        //					epicLog(LOG_INFO, "read locked while cache write(%d)", wr->op);
        //					if(wr->flag & TRY_LOCK) {
        //						return -1;
        //					} else {
        //						worker->to_serve_local_requests[i].push(wr);
        //						return 1;
        //					}
        //				}

        WorkRequest *lwr = new WorkRequest(*wr);
        lwr->counter = 0;
        lwr->op = WRITE_PERMISSION_ONLY; // diff
        lwr->flag |= CACHED;
        lwr->flag |= LOCKED;
        lwr->addr = i;
        lwr->size = BLOCK_SIZE;
        lwr->ptr = cline->line; // diff
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
        // to intermediate state
        epicAssert(state != CACHE_TO_DIRTY);
        ToToDirty(cline);
        worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);

#ifdef USE_LRU
        // we unlink the cache to avoid it is evicted before
        // the reply comes back
        // in which case it will cause error
        UnLinkLRU(cline);
#endif
      }
      else
      {
#ifdef USE_LRU
        UnLinkLRU(cline);
        LinkLRU(cline);
#endif

        if (WLock(cline, wr->addr))
        { // failed to lock

          wr->is_cache_hit_ = false;
          epicLog(LOG_INFO, "cannot exclusive lock addr %lx, will try later", wr->addr);

          if (wr->flag & TRY_LOCK)
          {
            wr->status = LOCK_FAILED;
            unlock(i);
            wr->unlock();
            return SUCCESS;
          }
          else
          {
            // to_serve_local_requests[TOBLOCK(wr->addr)].push(wr);
            worker->AddToServeLocalRequest(i, wr);
            unlock(i);
            wr->unlock();
            return IN_TRANSITION;
          }
        }
      }
    }
    else
    {
      epicLog(LOG_WARNING, "unknown op in cache operations");
    }
  }
  else
  {
    newcline++;
#ifdef SELECTIVE_CACHING
    if (!cline)
    {
      cline = SetCLine(i);
    }
    else
    {
      InitCacheCLine(cline);
    }
#else
    cline = SetCLine(i);
#endif
    wr->is_cache_hit_ = false;
    WorkRequest *lwr = new WorkRequest(*wr);
    // we hide the fact that it is whether a lock op or read/write from the remote side
    // as lock is completely maintained locally
    lwr->op = wr->op == RLOCK ? READ : WRITE;
    lwr->counter = 0;
    lwr->flag |= CACHED;
    lwr->flag |= LOCKED;
    lwr->addr = i;
    lwr->size = BLOCK_SIZE;
    lwr->ptr = cline->line;
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
    // to intermediate state
    if (RLOCK == wr->op)
    {
      epicAssert(cline->state != CACHE_TO_SHARED);
      ToToShared(cline);
    }
    else
    { // WLOCK
      epicAssert(cline->state != CACHE_TO_DIRTY);
      ToToDirty(cline);
    }
    worker->SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
  }
  int ret = wr->counter;
  unlock(i);
  wr->unlock();
#ifdef USE_LRU
  if (newcline)
    Evict(newcline);
#endif
  return ret;
}

int Cache::Read(WorkRequest *wr)
{
  epicAssert(wr->op == READ);
  return ReadWrite(wr);
}

int Cache::Write(WorkRequest *wr)
{
  epicAssert(wr->op == WRITE);
  return ReadWrite(wr);
}

int Cache::RLock(WorkRequest *wr)
{
  epicAssert(RLOCK == wr->op);
  return Lock(wr);
}

int Cache::WLock(WorkRequest *wr)
{
  epicAssert(WLOCK == wr->op);
  return Lock(wr);
}

Cache::Cache(Worker *w)
    : to_evicted(0),
      read_miss(0),
      write_miss(0),
      used_bytes(0)
#ifdef SELECTIVE_CACHING
      ,
      nread(0),
      nwrite(0),
      ntoshared(0),
      ntoinvalid(0)
#endif
{
  this->worker = w;
  max_cache_mem = w->conf->cache_th * w->conf->size;
}

void Cache::SetWorker(Worker *w)
{
  this->worker = w;
  max_cache_mem = w->conf->cache_th * w->conf->size;
}

void *Cache::GetLine(GAddr addr)
{
  GAddr block = GTOBLOCK(addr);
  if (caches.count(block))
  {
    CacheLine *cline = caches.at(block);
    epicAssert(GetState(cline) != CACHE_INVALID);
    return cline->line;
  }
  return nullptr;
}

#ifdef USE_LRU
void Cache::LinkLRU(CacheLine *cline)
{
#ifdef USE_APPR_LRU
  cline->lru_clock = worker->GetClock();
#else
  int j, i;
  for (j = 0; j < sample_num; j++)
  {
    i = GetRandom(0, LRU_NUM);
    if (lru_locks_[i].try_lock())
    {
      epicAssert(cline != heads[i]);
      epicAssert((heads[i] && tails[i]) || (!heads[i] && !tails[i]));
      cline->pos = i;
      cline->prev = nullptr;
      cline->next = heads[i];
      if (cline->next)
        cline->next->prev = cline;
      heads[i] = cline;
      if (!tails[i])
        tails[i] = cline;
      lru_locks_[i].unlock();
      break;
    }
  }
  if (j == sample_num)
  {
    epicLog(LOG_WARNING,
            "cannot link to any random lru list by trying %d times",
            sample_num);
    for (j = 0; j < LRU_NUM; j++)
    {
      i = j;
      if (lru_locks_[i].try_lock())
      {
        epicAssert(cline != heads[i]);
        epicAssert((heads[i] && tails[i]) || (!heads[i] && !tails[i]));
        cline->pos = i;
        cline->prev = nullptr;
        cline->next = heads[i];
        if (cline->next)
          cline->next->prev = cline;
        heads[i] = cline;
        if (!tails[i])
          tails[i] = cline;
        lru_locks_[i].unlock();
        break;
      }
    }
    if (j == LRU_NUM)
    {
      epicLog(LOG_WARNING, "cannot link to any lru list (total lru list %d)",
              LRU_NUM);
      i = GetRandom(0, LRU_NUM);
      lru_locks_[i].lock();
      epicAssert(cline != heads[i]);
      epicAssert((heads[i] && tails[i]) || (!heads[i] && !tails[i]));
      cline->pos = i;
      cline->prev = nullptr;
      cline->next = heads[i];
      if (cline->next)
        cline->next->prev = cline;
      heads[i] = cline;
      if (!tails[i])
        tails[i] = cline;
      lru_locks_[i].unlock();
    }
  }
#endif
}

// by calling this, we assume that it already got the lock for lru_list[i]
void Cache::UnLinkLRU(CacheLine *cline, int i)
{
  epicAssert(i != -1);
  if (heads[i] == cline)
  {
    epicAssert(cline->prev == 0);
    heads[i] = cline->next;
  }
  if (tails[i] == cline)
  {
    epicAssert(cline->next == 0);
    tails[i] = cline->prev;
  }
  epicAssert(cline->next != cline);
  epicAssert(cline->prev != cline);

  if (cline->next)
    cline->next->prev = cline->prev;
  if (cline->prev)
    cline->prev->next = cline->next;

  cline->next = cline->prev = nullptr;
}

void Cache::UnLinkLRU(CacheLine *cline)
{
#ifdef USE_APPR_LRU
  return;
#endif
  if (cline->pos != -1)
  {
    int i = cline->pos;
    cline->pos = -1;
    lru_locks_[i].lock();
    UnLinkLRU(cline, i);
    lru_locks_[i].unlock();
  }
}

void Cache::Evict()
{
  epicLog(LOG_INFO,
          "used_bytes = %ld, max_cache_mem = %ld,  BLOCK_SIZE = %ld, th = %lf, to_evicted = %ld",
          used_bytes.load(), max_cache_mem, BLOCK_SIZE, max_cache_mem,
          worker->conf->cache_th, to_evicted.load());
  long long used = used_bytes - to_evicted * BLOCK_SIZE;
  if (used > 0 && used > max_cache_mem)
  {
    int n = (used - max_cache_mem) / BLOCK_SIZE;
    epicLog(LOG_INFO,
            "tryng to evict %d, used = %ld, max_cache_mem = %ld, used > max_cache_mem = %d",
            n, used, max_cache_mem, used > max_cache_mem);
    int ret = Evict(n);
    if (ret < n)
    {
      epicLog(LOG_INFO, "only able to evict %d, but expect to evict %d", ret, n);
    }
  }
}

/*
 * evict n cache lines if we cannot fit in n more new cache lines
 * return: true if we allow n more new cache lines
 * 		   false if we don't have enough free space for n more cache lines
 */
int Cache::Evict(int n)
{
  long long used = used_bytes - to_evicted * BLOCK_SIZE;
  if (used < 0 || used <= max_cache_mem)
    return 0;

  int max = (used - max_cache_mem) / BLOCK_SIZE;
  n = n > max ? max : n;
#ifdef USE_APPR_LRU
  int i = 0;
  int max_samples = 3;
  for (i = 0; i < n; i++)
  {
    for (int j = 0; j < max_samples; j++)
    {
      not finished yet
    }
  }
#else
  int i = 0;
  int tries = 1, tried = 0;
  int max_evict = 16;
  epicLog(LOG_INFO, "trying to evict %d, but max is %d", n, max_evict);
  if (n > max_evict)
    n = max_evict;
  GAddr addr = Gnullptr;
  for (i = 0; i < n; i++)
  {
    int lru_no = GetRandom(0, LRU_NUM);
    if (lru_locks_[lru_no].try_lock())
    {
      if (!tails[lru_no])
      {
        epicLog(LOG_INFO, "No cache exists");
        lru_locks_[lru_no].unlock();
        return 0;
      }
      CacheLine *to_evict = tails[lru_no];
      tried = 0;
      while (to_evict)
      { // only unlocked cache line can be evicted
        addr = to_evict->addr;
        if (try_lock(addr))
        {
          if (unlikely(to_evict->locks.size() || InTransitionState(to_evict)))
          {
            epicLog(LOG_INFO, "cache line (%lx) is locked", to_evict->addr);
            unlock(addr);
          }
          else
          {
            break;
          }
        }
        tried++;
        if (tried == tries)
        {
          to_evict = nullptr;
          break;
        }
        to_evict = to_evict->prev;
      }

      if (to_evict)
      {
        UnLinkLRU(to_evict, to_evict->pos); // since we already got the lock in the parent function of Evict(CacheLine*)
      }
      lru_locks_[lru_no].unlock();
      if (!to_evict)
      {
        epicLog(LOG_INFO, "all the cache lines are searched");
        continue;
      }
      epicAssert(!InTransitionState(to_evict));
      Evict(to_evict);
      unlock(addr);
    }
  }
  if (i < n)
    epicLog(LOG_WARNING, "trying to evict %d, but only evicted %d", n, i);
  return i;
#endif
}

void Cache::Evict(CacheLine *cline)
{
  epicLog(LOG_INFO, "evicting %lx", cline->addr);
  epicAssert(cline->addr == TOBLOCK(cline->addr));
  epicAssert(!IsBlockLocked(cline->addr));
  epicAssert(!InTransitionState(cline));
  epicAssert(caches.at(cline->addr) == cline);
  int state = cline->state;

  WorkRequest *wr = new WorkRequest();
  wr->addr = cline->addr;
  wr->ptr = cline->line;
  Client *cli = worker->GetClient(cline->addr);
  if (CACHE_SHARED == state)
  {
    wr->op = ACTIVE_INVALIDATE;
    ToInvalid(cline);
    worker->SubmitRequest(cli, wr);
    delete wr;
    wr = nullptr;
  }
  else if (CACHE_DIRTY == state)
  {
    wr->op = WRITE_BACK;
    cli->Write(cli->ToLocal(wr->addr), cline->line, BLOCK_SIZE);
    ToToInvalid(cline);
    //  UnLinkLRU(cline, pos); //since we already got the lock in the parent function of Evict(CacheLine*)
    worker->SubmitRequest(worker->GetClient(wr->addr), wr,
                          ADD_TO_PENDING | REQUEST_SEND);
  }
  else
  { // invalid
    epicAssert(CACHE_INVALID == state);
    epicLog(LOG_INFO, "unexpected cache state when evicting");
  }
}
#endif

CacheLine *Cache::SetCLine(GAddr addr, void *line)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  CacheLine *cl = nullptr;
  if (caches.count(block))
  {
    epicLog(LOG_INFO, "cache line for gaddr %lx already exist in the cache",
            addr);
    cl = caches.at(block);
    if (line)
    {
      worker->sb.sb_free((byte *)cl->line - CACHE_LINE_PREFIX);
      used_bytes -= (BLOCK_SIZE + CACHE_LINE_PREFIX);
      cl->line = line;
      cl->addr = block;
      epicLog(LOG_WARNING, "should not use for now");
    }
  }
  else
  {
    cl = new CacheLine();
    if (line)
    {
      cl->line = line;
      epicLog(LOG_WARNING, "should not use for now");
    }
    else
    {
      caddr ptr = worker->sb.sb_aligned_calloc(1,
                                               BLOCK_SIZE + CACHE_LINE_PREFIX);
      used_bytes += (BLOCK_SIZE + CACHE_LINE_PREFIX);
      //*(byte*) ptr = CACHE_INVALID;
      ptr = (byte *)ptr + CACHE_LINE_PREFIX;
      cl->line = ptr;
      cl->addr = block;
    }
    caches[block] = cl;
  }
  return cl;
}

void *Cache::SetLine(GAddr addr, caddr line)
{
  return SetCLine(addr, line)->line;
}

void Cache::ToShared(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    ToShared(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
  }
}

#ifdef SELECTIVE_CACHING
void Cache::ToNotCache(CacheLine *cline, bool write)
{
  cline->state = CACHE_NOT_CACHE;

  if (write)
    return;

  worker->sb.sb_free((char *)cline->line - CACHE_LINE_PREFIX);
  used_bytes -= (BLOCK_SIZE + CACHE_LINE_PREFIX);
  cline->line = nullptr;
}
#endif

void Cache::UndoShared(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    UndoShared(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
  }
}
/* add ergeda add */
void Cache::DeleteCache(CacheLine *cline)
{
  void *line = cline->line;
  worker->sb.sb_free((char *)line - CACHE_LINE_PREFIX);
  used_bytes -= (BLOCK_SIZE + CACHE_LINE_PREFIX);

  // epicAssert(!IsBlockLocked(cline)); //啥意思这句

  if (!caches.erase(cline->addr))
  {
    epicLog(LOG_WARNING, "cannot invalidate the cache line");
  }

  delete cline;
  cline = nullptr;
}
/* add ergeda add */

void Cache::ToInvalid(CacheLine *cline)
{
#ifdef SELECTIVE_CACHING
  cline->ntoinvalid++;
  ntoinvalid++;
#endif
  void *line = cline->line;
  worker->sb.sb_free((char *)line - CACHE_LINE_PREFIX);
 #ifdef SUB_BLOCK
  used_bytes -= (cline->CacheSize + CACHE_LINE_PREFIX);
#else
  used_bytes -= (BLOCK_SIZE + CACHE_LINE_PREFIX);
#endif

  epicAssert(!IsBlockLocked(cline));

#ifdef USE_LRU
  UnLinkLRU(cline);
#endif
  if (!caches.erase(cline->addr))
  {
    epicLog(LOG_WARNING, "cannot invalidate the cache line");
  }
#ifdef SELECTIVE_CACHING
  cline->state = CACHE_NOT_CACHE;
#else
  delete cline;
  cline = nullptr;
#endif
}

void Cache::ToInvalid(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  CacheLine *cline = nullptr;
  try
  {
    cline = caches.at(block);
    ToInvalid(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    epicAssert(false);
  }
}

void Cache::ToDirty(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    ToDirty(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
  }
}

/*
 * intermediate state
 * in transition from invalid to shared
 * READ Case 2
 */
void Cache::ToToShared(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    ToToShared(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
  }
}

/*
 * not used for now
 */
void Cache::ToToInvalid(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    ToToInvalid(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
  }
}

/*
 * intermediate state
 * in transition from invalid/shared to dirty
 * WRITE Case 3, 4 (invalid/shared to dirty)
 */
void Cache::ToToDirty(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    ToToDirty(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
  }
}

CacheState Cache::GetState(GAddr addr)
{
  epicAssert(addr == GTOBLOCK(addr));
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    return cline->state;
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    return CACHE_NOT_EXIST;
  }
}

bool Cache::InTransitionState(GAddr addr)
{
  CacheState s = GetState(addr);
  return InTransitionState(s);
}

int Cache::RLock(GAddr addr)
{
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    return RLock(cline, addr);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    return -1;
  }
}

int Cache::WLock(GAddr addr)
{
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    return WLock(cline, addr);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    return -1;
  }
}

bool Cache::IsWLocked(GAddr addr)
{
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    return IsWLocked(cline, addr);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    return false;
  }
}

bool Cache::IsRLocked(GAddr addr)
{
  GAddr block = GTOBLOCK(addr);
  try
  {
    CacheLine *cline = caches.at(block);
    return IsRLocked(cline, addr);
  }
  catch (const exception &e)
  {
    epicLog(LOG_WARNING, "Unexpected: cannot find the cache line");
    return false;
  }
}

void Cache::UnLock(GAddr addr)
{
  GAddr block = GTOBLOCK(addr);
  lock(block);
  try
  {
    CacheLine *cline = caches.at(block);
    epicAssert(cline->locks.count(addr));
    if (cline->locks.at(addr) == EXCLUSIVE_LOCK_TAG)
    { // exclusive lock
      cline->locks.erase(addr);
    }
    else
    {
      cline->locks.at(addr)--;
      if (cline->locks.at(addr) == 0)
        cline->locks.erase(addr);
    }
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    epicAssert(false);
  }
  unlock(block);
}

bool Cache::IsBlockLocked(GAddr block)
{
  epicAssert(GTOBLOCK(block) == block);
  try
  {
    CacheLine *cline = caches.at(block);
    return IsBlockLocked(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    return false;
  }
}

bool Cache::IsBlockWLocked(GAddr block)
{
  epicAssert(GTOBLOCK(block) == block);
  try
  {
    CacheLine *cline = caches.at(block);
    return IsBlockWLocked(cline);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "Unexpected: cannot find the cache line");
    return false;
  }
}

#ifdef SELECTIVE_CACHING
void Cache::InitCacheCLine(CacheLine *cline, bool write)
{
  cline->state = CACHE_INVALID;
  cline->line = nullptr;

  if (write)
    return;

  caddr ptr = worker->sb.sb_aligned_calloc(1, BLOCK_SIZE + CACHE_LINE_PREFIX);
  used_bytes += (BLOCK_SIZE + CACHE_LINE_PREFIX);
  //*(byte*) ptr = CACHE_INVALID;
  ptr = (byte *)ptr + CACHE_LINE_PREFIX;
  cline->line = ptr;
}

void Cache::InitCacheCLineIfNeeded(CacheLine *cline)
{
  if (!cline->line)
    InitCacheCLine(cline);
}
#endif
