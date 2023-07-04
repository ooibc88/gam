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

#ifdef NOCACHE
#include "remote_request_nocache.cc"
#else // not NOCACHE
#include "remote_request_cache.cc"
#endif // NOCACHE

#ifdef DHT
void Worker::ProcessRemoteHTable(Client *cli, WorkRequest *wr)
{
  epicLog(LOG_DEBUG, "node %d htable (%llu) recv'ed a remote htable request from %d", this->GetWorkerId(), htable, cli->GetWorkerId());
  wr->op = GET_HTABLE_REPLY;
  // epicAssert(htable != nullptr);
  if (htable != nullptr)
    wr->addr = this->ToGlobal(this->htable);
  else
    wr->addr = Gnullptr;
  wr->status = SUCCESS;
  this->SubmitRequest(cli, wr);
  delete wr;
}

void Worker::ProcessHTableReply(Client *cli, WorkRequest *wr)
{
  static std::map<uint32_t, GAddr> htableMap;
  int cnt = 0;

  if (cli)
  {
    htableMap[cli->GetWorkerId()] = wr->addr;
  }
  else
  {
    htableMap[this->GetWorkerId()] = ToGlobal(this->htable);
  }

  if (htableMap.size() == this->widCliMapWorker.size())
  {
    WorkRequest *pwr;
    if (cli)
      pwr = this->GetPendingWork(wr->id);
    else
      pwr = wr;
    char *a = (char *)pwr->addr;
    for (auto &p : htableMap)
    {
      if (p.second != Gnullptr)
      {
        a += appendInteger(a, p.second);
        cnt++;
      }
    }
    htableMap.clear();
    // pwr->ptr = (void*)addr;
    pwr->status = SUCCESS;
    if (cli)
      Notify(pwr);
  }

  if (cli)
    delete wr;
}
#endif // DHT

void Worker::ProcessRemoteMemStat(Client *client, WorkRequest *wr)
{
  uint32_t qp;
  int wid;
  Size mtotal, mfree;
  vector<Size> stats;
  Split<Size>((char *)wr->ptr, stats);
  epicAssert(stats.size() == wr->size * 4);
  for (int i = 0; i < wr->size; i++)
  {
    qp = stats[i * 4];
    wid = stats[i * 4 + 1];
    mtotal = stats[i * 4 + 2];
    mfree = stats[i * 4 + 3];

    Client *cli = FindClientWid(wid);
    widCliMapWorker[wid] = cli;
    //		if(GetWorkerId() == wid) {
    //			epicLog(LOG_DEBUG, "Ignore self information");
    //			continue;
    //		}
    cli->lock();
    if (cli)
    {
      cli->SetMemStat(mtotal, mfree);
    }
    else
    {
      epicLog(LOG_WARNING, "worker %d not registered yet", wid);
    }
    cli->unlock();
  }
  /*
   * we don't need wr any more
   */
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteMalloc(Client *client, WorkRequest *wr)
{
  void *addr = nullptr;
  if (wr->flag & ALIGNED)
  {
    addr = sb.sb_aligned_malloc(wr->size);
    epicAssert((uint64_t)addr % BLOCK_SIZE == 0);
  }
  else
  {
    addr = sb.sb_malloc(wr->size);
  }
  // FIXME: remove below
  memset(addr, 0, wr->size);
  if (addr)
  {
    wr->addr = TO_GLOB(addr, base, GetWorkerId());
    wr->status = SUCCESS;
    ghost_size += wr->size;
    if (ghost_size > conf->ghost_th)
      SyncMaster();
    epicLog(LOG_DEBUG,
            "allocated %d at address %lx, base = %lx, wid = %d, gaddr = %lx",
            wr->size, addr, base, GetWorkerId(), wr->addr);

    /* to do */
    /* add ergeda add */
    DataState Dstate = GetDataState(wr->flag);
    //    if (Dstate != MSI) {
    GAddr Owner = ((1ll * (int)(wr->arg)) << 48);
    //    if (Dstate != DataState::MSI) {
    CreateDir(wr, Dstate, Owner); // home_node需要建立directory
    //    }
    //    }
    /* add ergeda add */
  }
  else
  {
    wr->status = ALLOC_ERROR;
  }
  wr->op = MALLOC_REPLY;
  SubmitRequest(client, wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteMallocReply(Client *client, WorkRequest *wr)
{
  WorkRequest *pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  if (wr->status)
  {
    epicLog(LOG_WARNING, "remote malloc error");
  }
  pwr->addr = wr->addr;
  pwr->status = wr->status;
  //	pending_works.erase(wr->id);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  if (Notify(pwr))
  {
    epicLog(LOG_FATAL, "cannot wake up the app thread");
  }
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteGetReply(Client *client, WorkRequest *wr)
{
  WorkRequest *pwr = GetPendingWork(wr->id);
  epicAssert(pwr);
  pwr->status = wr->status;
  pwr->size = wr->size;

  if (wr->status)
  {
    epicAssert(!pwr->size);
    epicLog(LOG_WARNING, "cannot get the value for key %ld", wr->key);
  }
  else
  {
    memcpy(pwr->ptr, wr->ptr, wr->size);
  }

  if (Notify(pwr))
  {
    epicLog(LOG_FATAL, "cannot wake up the app thread");
  }
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteEvictShared(Client *client, WorkRequest *wr)
{
  void *laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry *entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry))
  {
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in transition state %d",
            directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  directory.Clear(entry, client->ToGlobal(wr->ptr));
  directory.unlock(laddr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteEvictDirty(Client *client, WorkRequest *wr)
{
  void *laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  DirEntry *entry = directory.GetEntry(laddr);
  if (directory.InTransitionState(entry))
  {
    // to_serve_requests[wr->addr].push(pair<Client*, WorkRequest*>(client, wr));
    AddToServeRemoteRequest(wr->addr, client, wr);
    epicLog(LOG_INFO, "directory in transition state %d",
            directory.GetState(entry));
    directory.unlock(laddr);
    return;
  }
  directory.Clear(entry, client->ToGlobal(wr->ptr));
  directory.unlock(laddr);
  client->WriteWithImm(nullptr, nullptr, 0, wr->id);
  delete wr;
  wr = nullptr;
}

/* add ergeda add */
void Worker::ProcessRemoteReadType(Client *client, WorkRequest *wr)
{
  // Just_for_test("ProcessRemoteReadType", wr);
  if (!IsLocal(wr->addr))
  {
    epicLog(LOG_WARNING, "bug , cannot get to home_node\n");
  }

  char buf[16];
  wr->ptr = buf;
  wr->size = 12;
  wr->op = TYPE_REPLY;

  void *laddr = ToLocal(TOBLOCK(wr->addr));
  epicLog(LOG_WARNING, "addr : %lld, laddr : %llx\n", TOBLOCK(wr->addr), laddr);
  directory.lock(laddr);
  DirEntry *entry = directory.GetEntry(laddr);
  DataState Dstate = directory.GetDataState(entry);
  GAddr Owner = directory.GetOwner(entry);
  directory.unlock(laddr);

  appendInteger(buf, (int)Dstate, Owner);
  SubmitRequest(client, wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteTypeReply(Client *client, WorkRequest *wr)
{
  // Just_for_test ("ProcessRemoteTypeReply", wr);
  int Tmp;
  GAddr Owner;
  readInteger((char *)wr->ptr, Tmp, Owner);
  DataState Dstate = (DataState)Tmp;

  GAddr addr = TOBLOCK(wr->addr);
  directory.lock((void *)addr);
  DirEntry *entry = directory.GetEntry((void *)addr);
  directory.SetDataState(entry, Dstate);
  directory.SetMetaOwner(entry, Owner);
  directory.SetShared(entry);
  directory.unlock((void *)addr);

  int ret = ErasePendingWork(wr->id);
  ProcessToServeRequest(wr);
  delete wr; // 合理究竟要不要加这句
  wr = nullptr;
}

void Worker::ProcessRemotePrivateReadReply(Client *client, WorkRequest *wr)
{
  WorkRequest *parent = wr->parent;
  parent->lock();

  GAddr pend = GADD(parent->addr, parent->size);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
  void *ls = (void *)((ptr_t)parent->ptr + GMINUS(gs, parent->addr));
  void *cs = (void *)((ptr_t)wr->ptr + GMINUS(gs, wr->addr));
  Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
  memcpy(ls, cs, len);

  if (--parent->counter == 0)
  { // read all the data
    parent->unlock();
    Notify(parent);
  }
  else
  {
    parent->unlock();
  }

  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemotePrivateWrite(Client *client, WorkRequest *wr)
{
  // Just_for_test("ProcessRemotePrivateWrite", wr);
  GAddr blk = TOBLOCK(wr->addr);
  if (IsLocal(wr->addr))
  { // home_node = owner_node
    void *laddr = ToLocal(wr->addr);
    directory.lock(ToLocal(blk)); // addr有可能不是block_size的整数倍
    memcpy(laddr, wr->ptr, wr->size);
    directory.unlock(ToLocal(blk));
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
    void *laddr = cline->line + GMINUS(wr->addr, blk);
    memcpy(laddr, wr->ptr, wr->size);
    cache.unlock(blk);
  }

  client->WriteWithImm(nullptr, nullptr, 0, wr->id); // 告诉request-node已经写完了。

  delete wr;
  wr = nullptr;
}

void Worker::ProcessRemoteSetCacheReply(Client *client, WorkRequest *wr)
{
  // Just_for_test("ProcessRemoteSetCacheReply", wr);
  WorkRequest *pwr = GetPendingWork(wr->id);

  pwr->status = SUCCESS;
  //	pending_works.erase(wr->id);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  if (Notify(pwr))
  {
    epicLog(LOG_FATAL, "cannot wake up the app thread");
  }
  delete wr;
  wr = nullptr;
}
/*
void whyO() {
  int a = 1, b = 2;
  printf ("%d\n", a + b);
}
*/
/* add ergeda add */

void Worker::ProcessRequest(Client *client, WorkRequest *wr)
{
  epicLog(LOG_DEBUG, "process remote request %d from worker %d", wr->op,
          client->GetWorkerId());
  epicAssert(wr->wid == 0 || wr->wid == client->GetWorkerId());

  switch (wr->op)
  {

#ifdef DHT
  case GET_HTABLE:
  {
    this->ProcessRemoteHTable(client, wr);
    break;
  }
  case GET_HTABLE_REPLY:
  {
    this->ProcessHTableReply(client, wr);
    break;
  }
#endif // DHT

  case FETCH_MEM_STATS_REPLY:
  case BROADCAST_MEM_STATS:
  {
    ProcessRemoteMemStat(client, wr);
    break;
  }
  case MALLOC:
  {
    ProcessRemoteMalloc(client, wr);
    break;
  }
  case MALLOC_REPLY:
  {
    ProcessRemoteMallocReply(client, wr);
    break;
  }
  case FREE:
  {
    // FIXME: check whether other nodes are sharing this data
    // issue a write request first, and then process the free
    epicAssert(IsLocal(wr->addr));
    Size size = sb.sb_free(ToLocal(wr->addr));
    ghost_size -= size;
    if (abs((long long)ghost_size.load()) > conf->ghost_th)
      SyncMaster();
    delete wr;
    wr = nullptr;
    break;
  }
  case GET_REPLY:
  {
    ProcessRemoteGetReply(client, wr);
    break;
  }
  case READ:
  {
    ProcessRemoteRead(client, wr);
    break;
  }
  case READ_FORWARD:
  case FETCH_AND_SHARED:
  {
    ProcessRemoteReadCache(client, wr);
    break;
  }
  case READ_REPLY:
  {
    ProcessRemoteReadReply(client, wr);
    break;
  }
  case WRITE:
  case WRITE_PERMISSION_ONLY:
  {
    ProcessRemoteWrite(client, wr);
    break;
  }
  case INVALIDATE:
  case FETCH_AND_INVALIDATE:
  case WRITE_FORWARD:
  case INVALIDATE_FORWARD:
  case WRITE_PERMISSION_ONLY_FORWARD:
  {
    ProcessRemoteWriteCache(client, wr);
    break;
  }
  case WRITE_REPLY:
  {
    ProcessRemoteWriteReply(client, wr);
    break;
  }
  case ACTIVE_INVALIDATE:
  {
    ProcessRemoteEvictShared(client, wr);
    break;
  }
  case WRITE_BACK:
  {
    ProcessRemoteEvictDirty(client, wr);
    break;
  }
  /* add ergeda add */
  case JUST_READ:
  {
    ProcessRemotePrivateRead(client, wr);
    break;
  }
  case READ_TYPE:
  {
    ProcessRemoteReadType(client, wr);
    break;
  }
  case TYPE_REPLY:
  {
    ProcessRemoteTypeReply(client, wr);
    break;
  }
  case JUST_READ_REPLY:
  {
    ProcessRemotePrivateReadReply(client, wr);
    break;
  }
  case JUST_WRITE:
  {
    ProcessRemotePrivateWrite(client, wr);
    break;
  }
  case SET_CACHE:
  {
    ProcessRemoteSetCache(client, wr);
    break;
  }
  case SET_CACHE_REPLY:
  {
    ProcessRemoteSetCacheReply(client, wr);
    break;
  }
  case RM_READ:
  {
    ProcessRemoteRmRead(client, wr);
    break;
  }
  case RM_WRITE:
  {
    ProcessRemoteRmWrite(client, wr);
    break;
  }
  case RM_FORWARD:
  {
    ProcessRemoteRmForward(client, wr);
    break;
  }
  case TEST_RDMA:
  {
    TestRecv(client, wr);
    break;
  }
  case WE_READ:
  {
    ProcessRemoteWeRead(client, wr);
    break;
  }
  case WE_WRITE:
  {
    ProcessRemoteWeWrite(client, wr);
    break;
  }
  case WE_INV:
  {
    ProcessRemoteWeInv(client, wr);
    break;
  }
    /*  add ergeda add */

    /* add wpq add */
  case ChangeSubLog:
  {
    ProcessRemoteChangeSubLog(client, wr);
    break;
  }
  case writeshared_READ:
  {
    ProcessRemoteWriteSharedRead(client, wr);
    break;
  }
  case InitAcquire:
  {
    ProcessRemoteInitAcquire(client, wr);
    break;
  }
  /* add wpq add */
#ifdef NOCACHE
  case RLOCK:
    ProcessRemoteRLock(client, wr);
    break;
  case WLOCK:
    ProcessRemoteWLock(client, wr);
    break;
  case RLOCK_REPLY:
  case WLOCK_REPLY:
    ProcessRemoteLockReply(client, wr);
    break;
  case UNLOCK:
    ProcessRemoteUnLock(client, wr);
    break;
#ifndef ASYNC_UNLOCK
  case UNLOCK_REPLY:
    ProcessRemoteUnLockReply(client, wr);
    break;
#endif
#endif
  default:
    epicLog(LOG_WARNING, "unrecognized request from %d",
            client->GetWorkerId());
    exit(-1);
    break;
  }
}

/* add ergeda add */
void Worker::TestSend(GAddr addr)
{
  Client *cli = GetClient(addr);
  WorkRequest *wr = new WorkRequest();
  wr->op = TEST_RDMA;
  CacheLine *cline = nullptr;
  cline = cache.SetCLine(addr);
  wr->ptr = cline->line;
  wr->addr = addr;
  SubmitRequest(cli, wr, ADD_TO_PENDING | REQUEST_SEND);
}
void Worker::TestPending()
{
}
void Worker::TestRecv(Client *client, WorkRequest *wr)
{
  if (wr->addr == 0)
  {
    // epicLog(LOG_WARNING, "submitrequest, recv\n");
    delete wr;
    wr = nullptr;
    return;
  }
  // client->WriteWithImm(wr->ptr, ToLocal(wr->addr), BLOCK_SIZE, wr->id);
  wr->addr = 0;
  // for (int i = 0; i < 100000; ++i) SubmitRequest(client, wr);
  client->WriteWithImm(nullptr, nullptr, 0, wr->id, wr->id, true);
}
void Worker::TestProcessing()
{
}
/* add ergeda add */

/* add wpq add */
void Worker::ProcessRemoteChangeSubLog(Client *client, WorkRequest *wr)
{
  epicLog(LOG_PQ, "wr->flagSub1 = %d, wr->flagSub2 = %d", wr->flagSub1, wr->flagSub2);
  epicLog(LOG_PQ, "wr->op = %d", wr->op);

  epicLog(LOG_PQ, "wr->addr = %lx", wr->addr);

  void *laddr;
  DirEntry *Entry;
  if (IsLocal(wr->addr))
  {
    laddr = ToLocal(wr->addr);
    Entry = directory.GetEntry(laddr);
  }
  else
  {
    laddr = (void *)wr->addr;
    Entry = directory.GetEntry(laddr);
  }

  epicAssert(Entry != nullptr);
  Entry->ownerlist_subblock[wr->flagSub1] = wr->flagSub2;

  for (int i = 0; i < 2; i++)
  {
    epicLog(LOG_PQ, "Entry->ownerlist_subblock[%d] = %d", i, Entry->ownerlist_subblock[i]);
  }
}

/* add wpq add */