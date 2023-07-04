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

#ifdef NOCACHE
#include "local_request_nocache.cc"
#else
#include "local_request_cache.cc"
#endif

#ifdef DHT
int Worker::ProcessLocalHTable(WorkRequest *pwr)
{
  pwr->id = this->GetWorkPsn();

  uint64_t mem = this->FindClientWid(GetWorkerId())->GetTotalMem();
  if (mem > 0)
    this->ProcessHTableReply(NULL, pwr);

  bool local_only = true;
  for (auto &entry : widCliMapWorker)
  {
    if (entry.first != this->GetWorkerId())
    {
      this->SubmitRequest(entry.second, pwr);
      local_only = false;
    }
  }

  if (!local_only)
  {
    AddToPending(pwr->id, pwr);
    return REMOTE_REQUEST;
  }
  else
    return SUCCESS;
}
#endif // DHT

int Worker::ProcessLocalMalloc(WorkRequest *wr)
{
  epicAssert(!(wr->flag & ASYNC));
  if ((wr->flag & REMOTE) || (wr->addr && !IsLocal(wr->addr)))
  { // remote alloc
    Client *cli = GetClient(wr->addr);
    if (!cli)
    {
      // wr->status = ALLOC_ERROR;
      epicLog(LOG_WARNING,
              "there is no remote worker, we allocate locally instead");
    }
    else
    {
      cli->SetMemStat(cli->GetTotalMem(), cli->GetFreeMem() - wr->size);
      SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
      return REMOTE_REQUEST;
    }
  }
  else if (wr->flag & RANDOM)
  {
    size_t size = GetWorkersSize();
    static unsigned int seed = GetWorkerId();
    int i;
    Client *cli = nullptr;
    for (i = 0; i < size; i++)
    {
      cli = nullptr;
      int workid = GetRandom(0, size, &seed);
      epicLog(LOG_WARNING, "workid = %d, size = %d", workid, size);
      auto it = widCliMapWorker.begin();
      while (workid--)
        it++;
      epicAssert(it != widCliMapWorker.end());
      cli = it->second;
      if (cli->GetWorkerId() == GetWorkerId())
      { // local allocation
        cli = nullptr;
        break;
      }
      if (wr->size <= cli->GetFreeMem())
      {
        break;
      }
    }
    if (i == size)
    {                            // cannot find a suitable client, try remote scheme again (rare case)
      cli = GetClient(wr->addr); // wr->addr = null
    }
    if (cli)
    {
      cli->SetMemStat(cli->GetTotalMem(), cli->GetFreeMem() - wr->size);
      SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
      return REMOTE_REQUEST;
    }
  }
  // local alloc
  // we reserve a minimum conf->cache_th size for cache
  if (cache.GetUsedBytes() + sb.get_avail() < conf->size * conf->cache_th)
  {
    Client *cli = GetClient();
    cli->lock();
    if (cli)
    {
      epicLog(LOG_DEBUG, "allocate remotely at worker %d", cli->GetWorkerId());
      Size free = cli->GetFreeMem();
      cli->SetMemStat(cli->GetTotalMem(), free - wr->size); // update memory stats
      cli->unlock();
      SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
      return REMOTE_REQUEST;
    }
    else
    {
      // no remote worker, we have no choice expect allocate locally
      epicLog(LOG_WARNING, "local memory pressure, but there is no remote worker");
      cli->unlock();
    }
  }

  void *addr;
  if (wr->flag & ALIGNED)
  {
    addr = sb.sb_aligned_malloc(wr->size);
    epicAssert((uint64_t)addr % BLOCK_SIZE == 0);
  }
  else
  {
    addr = sb.sb_malloc(wr->size);
    epicLog(LOG_DEBUG, "allocate addr at %lx", addr);
  }
  // FIXME: remove below
  memset(addr, 0, wr->size);
  if (addr)
  {
    wr->addr = TO_GLOB(addr, base, GetWorkerId());
    wr->status = SUCCESS;
    ghost_size += wr->size;
    if (abs((long long)ghost_size.load()) > conf->ghost_th)
      SyncMaster();
  }
  else
  {
    wr->status = ALLOC_ERROR;
  }

  /* add ergeda add */
  DataState Dstate = GetDataState(wr->flag);
  // if (Dstate != MSI) {
  GAddr Owner = ((long long)(wr->arg) << 48);
  // printf ("arg : %llu\n", wr->arg);
  // epicLog(LOG_WARNING, "owner_id : %d", WID(Owner) );
  if (GetWorkerId() == WID(wr->addr))
  {                               // Cur_node == home_node
    CreateDir(wr, Dstate, Owner); // home_node需要建立directory，只要选择多级一致性就需要在home_node建目录,unless msi
  }
  if (Dstate == DataState::ACCESS_EXCLUSIVE || Dstate == WRITE_EXCLUSIVE)
  {
    if (GetWorkerId() == WID(Owner) && GetWorkerId() != WID(wr->addr))
    { // cur_node = owner_node, cur_node != home_node, 需要在本地建cache
      CreateCache(wr, Dstate);
    }

    if (GetWorkerId() != WID(Owner) && WID(wr->addr) != WID(Owner))
    { // owner_node != home_node and owner_node != cur_node;
      // Cur_node != owner_node,需要发消息给owner_node，建立相应目录和cache.
      wr->op = SET_CACHE;
      // TODO: 后续工作需要加上一个指令，在状态转换的过程中，还需要传数据过去，可能通过传地址，然后用rdma的read_with_imm去实现
      Client *Cur_client = GetClient(Owner);
      // Just_for_test(wr);
      SubmitRequest(Cur_client, wr, ADD_TO_PENDING | REQUEST_SEND);
      return REMOTE_REQUEST; // 等待请求返回
    }
  }
  if (Dstate == DataState::WRITE_SHARED)
  {
    epicLog(LOG_PQ, "malloc a write_shared data at addr = %lx ,size = %d", wr->addr, wr->size);
  }
  if (Dstate == DataState::RC_WRITE_SHARED)
  {
    epicLog(LOG_DEBUG, "malloc a rc_write_shared data at addr = %lx ,size = %d", wr->addr, wr->size);
  }

  epicLog(LOG_WARNING, "at least got to malloc");
  /* add ergeda add */

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

// FIXME: check whether other nodes are sharing this data
// issue a write request first, and then process the free
int Worker::ProcessLocalFree(WorkRequest *wr)
{
  epicAssert(!(wr->flag & ASYNC));
  // TODO: whether need to invalidate the cached copies
  // don't need to invalidate as other data co-located within the same block may be still in use.
  epicAssert(wr->addr);
  if (IsLocal(wr->addr))
  {
    void *addr = ToLocal(wr->addr);
    Size size = sb.sb_free(addr);
    ghost_size -= size;
    if (abs((long long)ghost_size.load()) > conf->ghost_th)
      SyncMaster();
  }
  else
  {
    Client *cli = GetClient(wr->addr);
    if (!cli)
    {
      wr->status = ALLOC_ERROR;
    }
    else
    {
      SubmitRequest(cli, wr);
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
#endif
  return SUCCESS;
}

int Worker::ProcessLocalMFence(WorkRequest *wr)
{
  epicAssert(!(wr->flag & FENCE));
  Fence *fence = fences_.at(wr->fd);
  fence->lock();
  if (unlikely(IsFenced(fence, wr)))
  {
    epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
            fence->mfenced, fence->sfenced, fence->pending_works.size());
    AddToFence(fence, wr);
    fence->unlock();
  }
  else
  {
    if (fence->pending_writes)
    { // we only mark fenced when there are pending writes
      fence->mfenced = true;
      epicLog(LOG_DEBUG, "mfenced!!, pending_writes = %d",
              fence->pending_writes.load());
    }
    fence->unlock();
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
  }
  return SUCCESS;
}

int Worker::ProcessLocalSFence(WorkRequest *wr)
{
  epicAssert(!(wr->flag & FENCE));
  // TODO: add the sfence support
  epicLog(LOG_WARNING, "SFENCE is not supported for now!");
  Fence *fence = fences_.at(wr->fd);
  fence->lock();
  if (IsFenced(fence, wr))
  {
    epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
            fence->mfenced, fence->sfenced, wr->op);
    AddToFence(fence, wr);
    fence->unlock();
  }
  else
  {
    if (fence->pending_writes)
    { // we only mark fenced when there are pending writes
      fence->sfenced = true;
      epicLog(LOG_DEBUG, "sfenced!");
    }
    fence->unlock();
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
  }
  return SUCCESS;
}

/* add ergeda add */
void Worker::CreateDir(WorkRequest *wr, DataState Cur_state, GAddr Owner)
{
  // Just_for_test("CreateDir", wr);
  GAddr start = wr->addr;
  GAddr start_blk = TOBLOCK(start);
  GAddr end = GADD(start, wr->size);

  for (GAddr i = start_blk; i < end;)
  {
    GAddr nextb = BADD(i, 1);
    void *laddr;
    if (IsLocal(i))
      laddr = ToLocal(i);
    else
      laddr = (void *)i;
    directory.lock(laddr);
    directory.CreateEntry(laddr, Cur_state, Owner);
    directory.unlock(laddr);
    i = nextb;
  }
}
/* add ergeda add */

int Worker::ProcessLocalFlushToHome(WorkRequest *wr)
{
  // 获取addr的home_node
  int home_id = WID(wr->addr);
  GAddr home_addr = EMPTY_GLOB(home_id);
  Client *home_client = GetClient(home_addr);
  void *dest = home_client->ToLocal_flush(wr->addr);
  // void *dest = (void *)((wr->addr) & 0xFFFFFFFFFFFFL);
  void *src = GetCacheLocal(wr->addr);
  int size = 4;
  int workId = GetWorkerId();
  int id = wr->id;
  epicLog(LOG_DEBUG, "workId = %d, home_id = %d, addr = %lx,dest = %lx, src = %lx, size = %d, id = %d", workId, home_id, wr->addr, dest, src, size, id);
  // workid=2/3, homeid=1,
  FlushToHome(home_id, dest, src, size, id);

  return SUCCESS;
}

int Worker::ProcessLocalInitAcquire(WorkRequest *wr)
{
  epicAssert(InitAcquire == wr->op);
  int newcline = 0;
  GAddr start_blk = TOBLOCK(wr->addr);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr end_blk = TOBLOCK(end - 1);

  Client *cli = GetClient(wr->addr);
  GAddr start = wr->addr;

  wr->lock();

  for (GAddr i = start_blk; i < end;)
  {
    GAddr nextb = BADD(i, 1);
    
    cache.lock(i);
    CacheLine *cline = nullptr;

    WorkRequest *lwr = new WorkRequest(*wr);
    newcline++;
    cline = cache.SetCLine(i);

    lwr->addr = i;
    lwr->counter = 0;
    lwr->size = BLOCK_SIZE;
    lwr->ptr = cline->line;
    lwr->parent = wr;
    wr->counter++;

    SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);

    cache.unlock(i);
    i = nextb;
  }

  wr->unlock();

  return 1;
}

int Worker::ProcessLocalRequest(WorkRequest *wr)
{
  epicLog(
      LOG_DEBUG,
      "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
      wr->op, wr->flag, wr->addr, wr->size, wr->fd);
  int ret = SUCCESS;
  if (MALLOC == wr->op)
  {
    ret = ProcessLocalMalloc(wr);
  }
  else if (FREE == wr->op)
  {
    ret = ProcessLocalFree(wr);
  }
  else if (READ == wr->op)
  {
    ret = ProcessLocalRead(wr);
  }
  else if (WRITE == wr->op)
  {
#ifdef ReleaseConsistency
    ret = ProcessLocalWrite_RC(wr);
#else
    ret = ProcessLocalWrite(wr);
#endif
  }
  else if (MFENCE == wr->op)
  {
    ret = ProcessLocalMFence(wr);
  }
  else if (SFENCE == wr->op)
  {
    ret = ProcessLocalSFence(wr);
  }
  else if (RLOCK == wr->op)
  { // fence for every lock
    ret = ProcessLocalRLock(wr);
  }
  else if (WLOCK == wr->op)
  {
    ret = ProcessLocalWLock(wr);
  }
  else if (UNLOCK == wr->op)
  {
    ret = ProcessLocalUnLock(wr);
  }
  else if (GET == wr->op)
  {
    SubmitRequest(master, wr, ADD_TO_PENDING | REQUEST_SEND);
    ret = REMOTE_REQUEST;
  }
  else if (PUT == wr->op)
  {
    SubmitRequest(master, wr);
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
    ret = SUCCESS;

#ifdef DHT
  }
  else if (GET_HTABLE == wr->op)
  {
    ret = this->ProcessLocalHTable(wr);
#endif
  }
  else if (flushToHomeOp == wr->op)
  {
    ret = ProcessLocalFlushToHome(wr);
  }
  else if (InitAcquire == wr->op)
  {
    ret = ProcessLocalInitAcquire(wr);
  }
  else
  {
    wr->status = UNRECOGNIZED_OP;
    epicLog(LOG_WARNING, "unrecognized op %d from local thread %d", wr->op,
            wr->fd);
    exit(-1);
  }
  return ret;
}

void Worker::ProcessLocalRequest(aeEventLoop *el, int fd, void *data,
                                 int mask)
{
  char buf[1];
  if (1 != read(fd, buf, 1))
  {
    epicLog(LOG_WARNING, "read pipe failed (%d:%s)", errno, strerror(errno));
  }
  epicLog(LOG_DEBUG, "receive local request %c", buf[0]);

  Worker *w = (Worker *)data;
  WorkRequest *wr;
  int i = 0;
  while (w->wqueue->pop(wr))
  {
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
