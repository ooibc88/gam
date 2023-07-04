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

#ifdef DHT
#include "../dht/kv.h"
#endif

Worker *WorkerFactory::server = nullptr;

Worker::Worker(const Conf &conf, RdmaResource *res)
    : st(),
      wr_psn(),
      ghost_size(),
      wqueue(new boost::lockfree::queue<WorkRequest *>(INIT_WORKQ_SIZE))
#ifdef ASYNC_RDMA_SEND
      ,
      rdma_queue(new boost::lockfree::queue<RDMASendData *>(INIT_WORKQ_SIZE))
#endif
#ifdef USE_BOOST_THREADPOOL
      ,
      work(ioService)
#endif
{
  epicAssert(wqueue->is_lock_free());
  this->conf = &conf;
  cache.SetWorker(this);

  // get the RDMA resource
  if (res)
  {
    resource = res;
  }
  else
  {
    resource = RdmaResourceFactory::getWorkerRdmaResource();
  }

  // create the event loop
  el = aeCreateEventLoop(
      conf.maxthreads + conf.maxclients + EVENTLOOP_FDSET_INCR);

  // open the socket for listening to the connections from workers to exch rdma resouces
  char neterr[ANET_ERR_LEN];
  char *bind_addr =
      conf.worker_bindaddr.length() == 0 ? nullptr : const_cast<char *>(conf.worker_bindaddr.c_str());
  sockfd = anetTcpServer(neterr, conf.worker_port, bind_addr, conf.backlog);
  if (sockfd < 0)
  {
    epicLog(LOG_WARNING, "Opening port %d (bind_addr %s): %s", conf.worker_port,
            bind_addr, neterr);
    exit(1);
  }

  // register tcp event for rdma parameter exchange
  if (sockfd > 0 && aeCreateFileEvent(el, sockfd, AE_READABLE, AcceptTcpClientHandle, this) == AE_ERR)
  {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }

#ifndef RDMA_POLL
  // register rdma event
  if (resource->GetChannelFd() > 0 && aeCreateFileEvent(el, resource->GetChannelFd(), AE_READABLE, ProcessRdmaRequestHandle, this) == AE_ERR)
  {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }
#endif

  // register the local memory space used for allocation
  void *addr = sb.slabs_init(conf.size, conf.factor, true);
  epicAssert((ptr_t)addr == TOBLOCK(addr));
  RegisterMemory(addr, conf.size);

  this->log = new Log(addr);

#ifdef DHT
  if (NBKT * BKT_SIZE < (1.0 - conf.cache_th) * conf.size)
    htable = sb.sb_aligned_malloc(NBKT * BKT_SIZE, BKT_SIZE);
  if (htable == nullptr)
    epicPanic("Unable to allocate hash table!!!!");
#endif

  // connect to the master
  master = this->NewClient(true);
  master->ExchConnParam(conf.master_ip.c_str(), conf.master_port, this);
  SyncMaster();                // send the local stats to master
  SyncMaster(FETCH_MEM_STATS); // fetch the mem states of other workers from master

#ifdef USE_LOCAL_TIME_EVENT
  if (aeCreateTimeEvent(el, conf.timeout, LocalRequestChecker, this, NULL))
  {
    epicPanic("Unrecoverable error creating time event.");
  }
#endif

#ifdef USE_LRU
  if (aeCreateTimeEvent(el, conf.eviction_period, CacheEvictor, this, NULL))
  {
    epicPanic("Unrecoverable error creating time event.");
  }
#endif

  epicLog(LOG_INFO, "worker %d started\n", GetWorkerId());
#if defined(USE_LRU) // and defined(FARM_ENABLED)
  epicLog(LOG_WARNING, "LRU eviction is enabled, max cache lines = %d, "
                       "reserved mem size = %ld, cache_percentage = %f, block_size = %d",
          (int)(conf.size * conf.cache_th / BLOCK_SIZE), conf.size, conf.cache_th, BLOCK_SIZE);
#endif

#ifdef USE_BOOST_THREADPOOL
  // add two threads to the threadpool
  threadpool.create_thread(
      boost::bind(&boost::asio::io_service::run, &ioService));
  threadpool.create_thread(
      boost::bind(&boost::asio::io_service::run, &ioService));
#endif

  // create the Worker thread to start service
  // #if defined(MULTITHREAD)
  //     this->st = new thread(startEventLoop, el);
  // #else
  //     this->st = new thread(StartService, this);
  // #endif
#ifdef RDMA_POLL
  new thread(startEventLoop, el);
  sleep(2); // wait for initialization complete
#endif
  this->st = new thread(StartService, this);
#if defined(ASYNC_RDMA_SEND)
  new thread(AsyncRdmaSendThread, this);
#endif
}

#ifdef ASYNC_RDMA_SEND
void Worker::AsyncRdmaSendThread(Worker *w)
{
  RDMASendData *data;
  while (true)
  {
    while (w->rdma_queue->pop(data))
    {
      epicLog(LOG_DEBUG, "To worker %d, buf = %p, len = %ld, id = %u, signaled = %d\n",
              data->client->GetWorkerId(), data->buf, data->len, data->id, data->signaled);
      int ret = data->client->Send(data->buf, data->len, data->id, data->signaled);
      delete data;
      data = nullptr;
      if (ret == -1)
      {
        break;
      }
    }
  }
}
#endif

void Worker::StartService(Worker *w)
{
  aeEventLoop *eventLoop = w->el;
  // start epoll
  eventLoop->stop = 0;
  WorkRequest *wr;
  int ne = 0;
  ibv_wc wc[MAX_CQ_EVENTS];
  ibv_cq *cq = w->resource->GetCompQueue();

  while (likely(!eventLoop->stop))
  {
#ifdef RDMA_POLL
    while (!(ne = ibv_poll_cq(cq, MAX_CQ_EVENTS, wc)))
      ;
    if (ne < 0)
    {
      epicLog(LOG_WARNING, "poll CQ failed: %d:%s", errno, strerror(errno));
    }
    epicLog(LOG_DEBUG, "get completion %d event", ne);
    for (int i = 0; i < ne; ++i)
    {
#ifdef MULTITHREAD_RECV
      w->ioService.post(boost::bind(RdmaHandler, w, wc[i]));
#else
      w->ProcessRdmaRequest(wc[i]);
#endif
    }
#else
    if (eventLoop->beforesleep != NULL)
    {
      eventLoop->beforesleep(eventLoop);
    }
    aeProcessEvents(eventLoop, AE_ALL_EVENTS | AE_DONT_WAIT);
#endif

#ifndef MULTITHREAD
#ifdef USE_BOOST_QUEUE
    while (w->wqueue->pop(wr))
    {
      epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
              wr->op, wr->flag, wr->addr, wr->size, wr->fd);
      w->ProcessLocalRequest(wr);
    }
#elif defined(USE_BUF_ONLY)
    for (volatile int *buf : w->nbufs)
    {
      if (*buf == 1)
      {
        wr = *(WorkRequest **)(buf + 1);
        epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
                wr->op, wr->flag, wr->addr, wr->size, wr->fd);
        if (wr->flag & ASYNC)
        {
          *buf = 2; // notify the app thread to return immediately before we process the request
        }
        else
        {
          *buf = 0;
        }
        w->ProcessLocalRequest(wr);
      }
    }
#else
    for (volatile int *buf : w->nbufs)
    {
      if (*buf == 1)
      {
        while (w->wqueue->pop(wr))
        {
          epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
                  wr->op, wr->flag, wr->addr, wr->size, wr->fd);
          w->ProcessLocalRequest(wr);
        }
      }
    }
#endif
#endif
  }

#ifdef USE_BOOST_THREADPOOL
  w->ioService.stop();
#endif
  // end the service
  aeDeleteEventLoop(w->el);
}

/*
 * we use the socket to get the existing workers
 * - guarantee a consistent join sequence of workers
 * - avoid duplicate connection initiations for the same bi-connection
 *
 */
int Worker::PostConnectMaster(int fd, void *data)
{
  char inmsg[MAX_WORKERS_STRLEN + 1];
  char outmsg[MAX_IPPORT_STRLEN + 1];

  epicLog(LOG_DEBUG, "waiting for master reply with worker list");

  /* waiting for server's response */
  int n = read(fd, inmsg, MAX_WORKERS_STRLEN);
  if (n <= 0)
  {
    epicLog(LOG_WARNING, "Failed to read worker ip/ports (%s)\n",
            strerror(errno));
    return -1;
  }
  inmsg[n] = '\0';
  epicLog(LOG_DEBUG, "inmsg = %s (n = %d, MAX_WORKERS_STRLEN = %d)", inmsg, n,
          MAX_WORKERS_STRLEN);

  n = sprintf(outmsg, "%s:%d", this->GetIP().c_str(), this->GetPort());
  if (n != write(fd, outmsg, n))
  {
    epicLog(LOG_WARNING, "send worker ip/port failed (%s)\n", strerror(errno));
    return -2;
  }
  epicLog(LOG_DEBUG, "send: %s; received: %s\n", outmsg, inmsg);

  vector<string> splits;
  Split(inmsg, splits, ',');
  for (string s : splits)
  {
    if (s.length() < 9)
      continue;
    vector<string> ip_port;
    Split(s, ip_port, ':');
    epicLog(LOG_INFO, "ip_port = %s", s.c_str());
    epicAssert(ip_port.size() == 2);

    Client *c = this->NewClient();
    c->ExchConnParam(ip_port[0].c_str(), atoi(ip_port[1].c_str()), this);
  }
  return 0;
}

void Worker::RegisterMemory(void *addr, Size s)
{
  base = addr;
  size = s;
  resource->RegLocalMemory(addr, s);
}

int Worker::RegisterHandle(int fd, aeFileProc *handle)
{
  // register local request event
  if (fd > 0 && aeCreateFileEvent(el, fd, AE_READABLE, ProcessLocalRequest, this) == AE_ERR)
  {
    epicPanic("Unrecoverable error creating pipe file event.");
    return -1;
  }
  return 0;
}

#ifndef USE_BOOST_QUEUE
int Worker::RegisterNotifyBuf(volatile int *notify_buf)
{
  // register local buf
  nbufs.push_back(notify_buf);
  return 0;
}

void Worker::DeRegisterNotifyBuf(volatile int *notify_buf)
{
  // register local buf
  nbufs.remove(notify_buf);
}
#endif

void Worker::DeRegisterHandle(int fd)
{
  aeDeleteFileEvent(el, fd, AE_READABLE);
}

int Worker::LocalRequestChecker(struct aeEventLoop *eventLoop, long long id,
                                void *clientData)
{
  Worker *w = (Worker *)clientData;
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
  if (i)
    epicLog(LOG_DEBUG, "pop %d from work queue", i);
  return w->conf->timeout;
}

/*
 * wr: the just-finished pending workrequest
 */
void Worker::ProcessToServeRequest(WorkRequest *wr)
{
  GAddr block = TOBLOCK(wr->addr);
  LOCK_MICRO(to_serve_local_requests, block);
  // process these pending local requests due to in transition state
  if (to_serve_local_requests.count(block))
  {
    auto *entry = to_serve_local_requests.at(block);
    queue<WorkRequest *> lq(*entry); // copy it to local queue so that we don't need to hold the lock after that
    int size = entry->size();
    to_serve_local_requests.erase(block);
    delete entry;
    entry = nullptr;
    UNLOCK_MICRO(to_serve_local_requests, block);

    for (int i = 0; i < size; i++)
    {
      WorkRequest *to_serve = lq.front();
      to_serve->flag |= TO_SERVE;
      epicAssert(to_serve);
      epicLog(LOG_DEBUG, "processed to-serve local request %d", to_serve->op);
      epicLog(
          LOG_DEBUG,
          "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d, wr->wid = %d\n",
          to_serve->op, to_serve->flag, to_serve->addr, to_serve->size,
          to_serve->fd, to_serve->wid);
      epicAssert(
          to_serve->fd != 0 && (to_serve->wid == 0 || to_serve->wid == GetWorkerId()));

#ifdef NOCACHE
      epicAssert(RLOCK == to_serve->op || WLOCK == to_serve->op);
#endif
      lq.pop();
      ProcessLocalRequest(to_serve);
    }
  }
  else
  {
    UNLOCK_MICRO(to_serve_local_requests, block);
  }

  LOCK_MICRO(to_serve_requests, block);
  // process these pending remote requests due to in transition state
  if (to_serve_requests.count(block))
  {
    auto *entry = to_serve_requests.at(block);
    int size = entry->size();
    queue<pair<Client *, WorkRequest *>> lq(*entry); // copy it to local queue so that we don't need to hold the lock after that
    to_serve_requests.erase(block);
    delete entry;
    entry = nullptr;
    UNLOCK_MICRO(to_serve_requests, block);

    for (int i = 0; i < size; i++)
    {
      // auto& to_serve = to_serve_requests[block].front();
      auto &to_serve = lq.front();
      epicLog(LOG_DEBUG, "processed to-serve remote request %d",
              to_serve.second->op);
      epicLog(
          LOG_DEBUG,
          "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
          to_serve.second->op, to_serve.second->flag, to_serve.second->addr,
          to_serve.second->size, to_serve.second->fd);
#ifdef NOCACHE
      epicAssert(RLOCK == to_serve.second->op || WLOCK == to_serve.second->op);
#else
      epicAssert(
          (READ == to_serve.second->op || READ_FORWARD == to_serve.second->op || FETCH_AND_SHARED == to_serve.second->op || WRITE == to_serve.second->op || WRITE_PERMISSION_ONLY == to_serve.second->op || INVALIDATE == to_serve.second->op || FETCH_AND_INVALIDATE == to_serve.second->op || WRITE_FORWARD == to_serve.second->op || INVALIDATE_FORWARD == to_serve.second->op || WRITE_PERMISSION_ONLY_FORWARD == to_serve.second->op || WRITE_BACK == to_serve.second->op || ACTIVE_INVALIDATE == to_serve.second->op));
#endif
#if !defined(NOCACHE) && !defined(SELECTIVE_CACHING)
      epicAssert(block == to_serve.second->addr);
#endif
      epicAssert(
          to_serve.first->GetWorkerId() != 0 && to_serve.first->GetWorkerId() != GetWorkerId());
      to_serve.second->flag |= TO_SERVE;
      ProcessRequest(to_serve.first, to_serve.second);
      lq.pop();
    }
  }
  else
  {
    UNLOCK_MICRO(to_serve_requests, block);
  }
}

void Worker::SyncMaster(Work op, WorkRequest *parent)
{
  WorkRequest *wr = new WorkRequest();
  wr->parent = parent;
  int ret;

  char buf[MAX_REQUEST_SIZE];
  int len = 0;
  wr->op = op;
  if (UPDATE_MEM_STATS == op)
  {
    if (abs(conf->cache_th - 1) < std::numeric_limits<double>::epsilon())
    {
      epicLog(LOG_WARNING, "found one CacheOnly instance with wid = %d",
              GetWorkerId());
      return;
    }
    size_t cache_size = conf->size * conf->cache_th;
    wr->size = conf->size - cache_size;
    wr->free = sb.get_avail() - (cache_size - cache.GetUsedBytes());
    SubmitRequest(master, wr);

    // TODO: whether needs to do it in a callback func?
    ghost_size = 0;
  }
  else if (FETCH_MEM_STATS == op)
  {
    SubmitRequest(master, wr);
  }
  else
  {
    epicLog(LOG_WARNING, "unrecognized sync master op");
    exit(-1);
  }

  delete wr;
  wr = nullptr;
}

size_t Worker::GetWorkersSize()
{
  return widCliMapWorker.size();
}

Client *Worker::GetClient(GAddr addr)
{
  Client *cli = nullptr;
  int wid = 0;
  // UpdateWidMap();
  // epicAssert(widCliMap.size() == qpCliMap.size());
  // LOCK_MICRO(widCliMap, 0);
  if (unlikely(widCliMap.size() == 0))
  {
    epicLog(LOG_WARNING, "#remote workers is 0!");
  }
  else
  {
    if (addr)
    {
      epicAssert(!IsLocal(addr));
      wid = WID(addr);
      try
      {
        cli = widCliMap.at(wid);
      }
      catch (const std::out_of_range &oor)
      {
        epicLog(LOG_WARNING, "cannot find the client for worker %d (%s)", wid,
                oor.what());
      }
    }
    else
    {
      // epicLog(LOG_DEBUG, "select a random server to allocate");
      // while ((wid = rand() % widCliMap.size() + 1) == GetWorkerId());
      epicLog(LOG_DEBUG, "select the server with most free memory to allocate");
      Size max = 0;
      for (auto &entry : widCliMapWorker)
      {
        epicLog(LOG_DEBUG, "worker %d, have %ld free out of %ld",
                entry.second->GetWorkerId(), entry.second->GetFreeMem(),
                entry.second->GetTotalMem());
        if (entry.first == GetWorkerId())
          continue;
        if (max < entry.second->GetFreeMem())
        {
          max = entry.second->GetFreeMem();
          wid = entry.first;
          cli = entry.second;
        }
      }
    }
    // cli = FindClientWid(wid);
    if (!cli)
    {
      epicLog(LOG_WARNING, "cannot find the client for addr (%d:%d)", wid,
              OFF(addr));
    }
  }
  // UNLOCK_MICRO(widCliMap, 0);
  return cli;
}

unsigned long long Worker::SubmitRequest(Client *cli, WorkRequest *wr, int flag,
                                         void *dest, void *src, Size size,
                                         uint32_t imm)
{
  /*
   * TODO: it is actually not necessary
   * can remove it after development
   */
  // Just_for_test("submitrequest", wr);
  wr->wid = GetWorkerId();

  if (!((wr->op & REPLY) || (flag & REQUEST_NO_ID)))
  {
    wr->id = GetWorkPsn();
    epicLog(LOG_DEBUG, "wr->id = %d", wr->id);
  }

  if (flag & ADD_TO_PENDING){
    AddToPending(wr->id, wr);
    epicLog(LOG_DEBUG, "add to pending, wr->id = %d", wr->id);
  }
  if (flag & REQUEST_WRITE_IMM)
  {
    // epicLog(LOG_WARNING, "should not use for now");
    if (flag & ADD_TO_PENDING)
    {
      cli->WriteWithImm(dest, src, size, imm, wr->id, true);
    }
    else
    {
      cli->WriteWithImm(dest, src, size, imm);
    }
  }
  else if (flag & REQUEST_SEND)
  {
#ifdef ASYNC_RDMA_SEND
    char *sbuf = (char *)zmalloc(MAX_REQUEST_SIZE);
    int len;
    int ret = wr->Ser(sbuf, len);
    epicAssert(!ret);
    RDMASendData *data = new RDMASendData(cli, sbuf, len);
    rdma_queue->push(data);
#else
    char *sbuf = cli->GetFreeSlot();
    bool busy = false;
    if (sbuf == nullptr)
    {
      busy = true;
      sbuf = (char *)zmalloc(MAX_REQUEST_SIZE);
      epicLog(LOG_INFO,
              "We don't have enough slot buf, we use local buf instead");
    }
    int len;
    int ret = wr->Ser(sbuf, len);
    epicAssert(!ret);
    if ((ret = cli->Send(sbuf, len)) != len)
    {
      epicAssert(ret == -1);
      epicLog(LOG_INFO, "sent failed: slots are busy");
    }
#endif
  }
  else
  {
    epicLog(LOG_WARNING, "unrecognized request type");
    exit(-1);
  }
  epicAssert((flag & REQUEST_NO_ID) xor (flag & ADD_TO_PENDING));
  return wr->id;
}

void Worker::CompletionCheck(unsigned int id)
{
  // LOCK_MICRO(pending_works, id);
  if (!pending_works.count(id))
  {
    // UNLOCK_MICRO(pending_works, id);
    return;
  }
  WorkRequest *wr = pending_works.at(id);
  pending_works.erase(id);
  // UNLOCK_MICRO(pending_works, id);
  epicAssert(wr->id == id);
  switch (wr->op)
  {
  case PENDING_INVALIDATE:
    epicLog(LOG_DEBUG, "start pending_invalidate");
    cache.lock(wr->addr);
    cache.ToInvalid(wr->addr);
    cache.unlock(wr->addr);
    ProcessToServeRequest(wr);
    delete wr;
    wr = nullptr;
    epicLog(LOG_DEBUG, "finish pending_invalidate");
    break;
  default:
    epicLog(LOG_WARNING, "Unrecognized work request for pending work %d",
            wr->op);
    break;
  }
}

void Worker::ProcessFenced(Fence *fence)
{
  if (!fence->in_process)
  {
    fence->lock();
    if (fence->pending_writes == 0)
    {
      if (fence->mfenced || fence->sfenced)
      {
        // delay the update of fence->mfenced/fence->sfenced
        // in order to block other thread to proceed to process while
        // we are processing the requests
        // fence->mfenced = false;
        // fence->sfenced = false;
        fence->in_process = true;
        WorkRequest *lwr;
        bool fenced = false;
        while (fence->pending_works.size() && (lwr =
                                                   fence->pending_works.front()))
        {
          // epicAssert(wr->fd == lwr->fd);
          epicLog(
              LOG_DEBUG,
              "processed fenced (mfenced = %d, sfenced = %d) op = %d, addr = %lx, fd = %d",
              fence->mfenced, fence->sfenced, lwr->op, lwr->addr, lwr->fd);
          if (lwr->op == MFENCE)
          {
            if (fence->pending_writes)
            {
              fence->mfenced = true;
              fence->sfenced = false;
              fenced = true;
              epicLog(LOG_DEBUG, "mfenced!!, pending_writes = %d",
                      fence->pending_writes.load());
              fence->pending_works.pop();
              break;
            }
            else
            {
              fence->pending_works.pop();
            }
          }
          else if (lwr->op == SFENCE)
          {
            if (fence->pending_writes)
            {
              fence->sfenced = true;
              fence->mfenced = false;
              fenced = true;
              epicLog(LOG_DEBUG, "sfenced!!, pending_writes = %d",
                      fence->pending_writes.load());
              fence->pending_works.pop();
              break;
            }
            else
            {
              fence->pending_works.pop();
            }
          }
          else if (lwr->op == RLOCK && fence->pending_writes)
          {
            fence->sfenced = false;
            fence->mfenced = true;
            fenced = true;
            epicLog(LOG_DEBUG, "mfenced due to RLOCK!!, pending_writes = %d",
                    fence->pending_writes.load());
            // do not pop it from fence->pending_works
            // otherwise the order of this request will be put in the back of the queue
            break;
          }
          else if (lwr->op == WLOCK && fence->pending_writes)
          {
            fence->sfenced = false;
            fence->mfenced = true;
            fenced = true;
            epicLog(LOG_DEBUG, "mfenced due to WLOCK!!, pending_writes = %d",
                    fence->pending_writes.load());
            // do not pop it from fence->pending_works
            // otherwise the order of this request will be put in the back of the queue
            break;
          }
          else if (lwr->op == UNLOCK && fence->pending_writes)
          {
            fence->sfenced = false;
            fence->mfenced = true;
            fenced = true;
            epicLog(LOG_DEBUG, "mfenced due to UNLOCK!!, pending_writes = %d",
                    fence->pending_writes.load());
            // do not pop it from fence->pending_works
            // otherwise the order of this request will be put in the back of the queue
            break;
          }
          else
          {
            epicAssert(lwr->flag & FENCE);
            ProcessLocalRequest(lwr);
            fence->pending_works.pop();
          }
          //					//TODO: if sfenced, we should allow read to pass
          //					if(fence->mfenced || fence->sfenced) {
          //						break;
          //					}
        }
        /*
         * we must not change the fenced status until last
         * since otherwise it may allow other thread to pass the fence and process request
         * before the fenced requests are processed,
         * which will cause re-ordering of request processing
         */
        if (!fenced)
        {
          fence->mfenced = false;
          fence->sfenced = false;
        }
        fence->in_process = false;
      }
    }
    fence->unlock();
  }
}

int Worker::Notify(WorkRequest *wr)
{
  // Just_for_test("Notify", wr);
  if (wr->op == WRITE || wr->op == WLOCK)
  {
    epicLog(LOG_DEBUG, "Notify and wr->op == WRITE || wr->op == WLOCK");
    if (IsLocal(wr->addr))
    {
      ++no_local_writes_;
      if (wr->is_cache_hit_)
        ++no_local_writes_hit_;
    }
    else
    {
      ++no_remote_writes_;
      epicLog(LOG_DEBUG, "++no_remote_writes_", no_remote_writes_.load());
      if (wr->is_cache_hit_)
      {
        ++no_remote_writes_hit_;
        epicLog(LOG_DEBUG, "++no_remote_writes_hit_", no_remote_writes_hit_.load());
      }
    }
  }

  if (wr->op == READ || wr->op == RLOCK)
  {
    if (IsLocal(wr->addr))
    {
      ++no_local_reads_;
      if (wr->is_cache_hit_)
        ++no_local_reads_hit_;
    }
    else
    {
      ++no_remote_reads_;
      if (wr->is_cache_hit_)
        ++no_remote_reads_hit_;
    }
  }

  if (wr->flag & ASYNC)
  {
    epicAssert(wr->op == WRITE || wr->op == MFENCE || wr->op == UNLOCK); // currently only writes, mfence and unlock are asynchronous
    Fence *fence = fences_.at(wr->fd);
    epicLog(LOG_DEBUG, "Asynchronous request notification, pending %d",
            fence->pending_writes.load());

    if (WRITE == wr->op)
      fence->pending_writes--;
    if (fence->pending_writes < 0)
    {
      epicLog(LOG_WARNING, "fence->pending_writes = %d",
              fence->pending_writes.load());
      epicAssert(false);
    }
    ProcessFenced(fence);
    if (wr->flag & COPY)
    {
      WorkRequest *o = wr;
      wr = wr->Copy();
      if (wr->ptr && wr->size)
        zfree(wr->ptr);
      delete wr;
      wr = nullptr;
    }
  }
  else
  {
#ifdef USE_PIPE_W_TO_H
    if (write(wr->fd, "r", 1) != 1)
    {
      epicLog(LOG_WARNING, "writing to pipe error (%d:%s)", errno, strerror(errno));
    }
#elif defined(USE_PTHREAD_COND)
    pthread_mutex_lock(wr->cond_lock);
    pthread_cond_signal(wr->cond);
    pthread_mutex_unlock(wr->cond_lock);
#else
#ifdef USE_BUF_ONLY
    epicAssert(*wr->notify_buf == 0);
#else
    // epicLog(LOG_WARNING, "wr->notify_buf = %d, wr = %lx, wr->notify_buf = %lx", *wr->notify_buf, wr, wr->notify_buf);
    epicAssert(*wr->notify_buf == 1);
#endif
    *wr->notify_buf = 2;
    //	epicAssert(*wr->notify_buf == 2); //DO NOT check here, user thread may already change its value!!!!
    epicLog(LOG_DEBUG, "writing to notify_buf");
#endif
  }

  return 0;
}

void Worker::AddToPending(unsigned int id, WorkRequest *wr)
{
  epicAssert(
      !(wr->flag & ASYNC) || !(wr->flag & LOCAL_REQUEST) || ((wr->flag & ASYNC) && (wr->flag & COPY)));
  epicAssert(id == wr->id);
  // LOCK_MICRO(pending_works, id);
  // epicAssert(!(wr->flag & ASYNC) || wr->IsACopy());
  epicLog(LOG_DEBUG, "add pending work %d, wr->op = %d, wr addr = %lx", id,
          wr->op, wr);
  pending_works[id] = wr;
  // UNLOCK_MICRO(pending_works, id);
}

int Worker::ErasePendingWork(unsigned int id)
{
  // LOCK_MICRO(pending_works, id);
  epicLog(LOG_DEBUG, "remove pending work %d", id);
  int ret = pending_works.erase(id);
  // UNLOCK_MICRO(pending_works, id);
  return ret;
}

WorkRequest *Worker::GetPendingWork(unsigned int id)
{
  // LOCK_MICRO(pending_works, id);
  WorkRequest *ret = nullptr;
  epicLog(LOG_DEBUG, "get pending work %d", id);
  try
  {
    ret = pending_works.at(id);
    epicAssert(ret);
  }
  catch (const exception &e)
  {
    epicLog(LOG_WARNING, "cannot find the pending work %d (%s)", id, e.what());
    // UNLOCK_MICRO(pending_works, id);
    return nullptr;
  }
  // UNLOCK_MICRO(pending_works, id);
  return ret;
}

int Worker::GetAndErasePendingWork(unsigned int id, WorkRequest **wp)
{
  // LOCK_MICRO(pending_works, id);
  epicLog(LOG_WARNING, "get and erase pending work %d", id);
  try
  {
    *wp = pending_works.at(id);
  }
  catch (const exception &e)
  {
    epicLog(LOG_FATAL, "cannot find the pending work %d (%s)", id, e.what());
    // UNLOCK_MICRO(pending_works, id);
    return 0;
  }
  int ret = pending_works.erase(id);
  // UNLOCK_MICRO(pending_works, id);
  return ret;
}

int Worker::FlushToHome(int workId, void *dest, void *src, int size, int id)
{
  epicLog(LOG_DEBUG, "flush to home");
  Client *client = FindClientWid(workId);

  if (client == nullptr)
  {
    epicLog(LOG_DEBUG, "cannot find the client for worker %d", workId);
    return -1;
  }
  // client->WriteWithImm(dest, src, size, id);
  client->Write(dest, src, size);
  return 0;
}

int Worker::releaseLock(GAddr addr)
{
  epicLog(LOG_DEBUG, "release lock");
  while (to_flush_list.empty() == false)
  {
    sleep(0.01);
    epicLog(LOG_DEBUG, "to_flush_list.empty() == false");
  }
  epicLog(LOG_DEBUG, "this_id = %ld,release is done", std::this_thread::get_id());
  flush_done = true;
  while(is_acquired.load() == true){
    sleep(0.01);
    epicLog(LOG_DEBUG, "is_acquired.load() == true");
  }
  
  return 0;
}

int Worker::ConsumerToFlushList()
{
  // 检测to_flush_list 队列中是否有元素
  while (true)
  {
    if (flush_done.load() == true)
    {
      epicLog(LOG_DEBUG, "flush done,stop thread id = %ld", std::this_thread::get_id());
      is_acquired = false;
      break;
    }
    
    if (to_flush_list.empty())
    {
      sleep(0.01);
    }
    else
    {
      epicLog(LOG_DEBUG, "to_flush_list is not empty,this_id = %ld, to_flush_list size = %d", std::this_thread::get_id(), to_flush_list.size());
      // 从队列中取出元素
      pair<GAddr, int> addr_size = to_flush_list.front();
      to_flush_list.pop();
      GAddr addr = addr_size.first;
      int size = addr_size.second;
      epicLog(LOG_DEBUG, "here here addr = %lx, size = %d,work_id = %d", addr, size, GetWorkerId());

      int home_id = WID(addr);
      GAddr home_addr = EMPTY_GLOB(home_id);
      Client *home_client = GetClient(home_addr);
      void *dest = home_client->ToLocal_flush(addr);
      void *src = GetCacheLocal(addr);
      int workId = GetWorkerId();
      int id = 0;
      FlushToHome(home_id, dest, src, size, id);
    }
  }
  return 0;
}

int Worker::acquireLock(GAddr addr, int size)
{
  epicAssert(is_acquired == false);
  epicLog(LOG_DEBUG, "acquire lock");
  is_acquired = true;
  flush_done = false;

  std::thread t1(&Worker::ConsumerToFlushList, this);
  t1.detach();
  return 0;
}

Worker::~Worker()
{
  aeDeleteEventLoop(el);
  delete wqueue;
  delete st;
  delete log;
  wqueue = nullptr;
  st = nullptr;
}
