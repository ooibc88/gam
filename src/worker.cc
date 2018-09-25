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
#include "settings.h"

#include "farm_txn.h"

Worker* WorkerFactory::server = nullptr;

void Worker::ProcessLocalRequest(aeEventLoop *el, int fd, void *data, int mask) {
  char buf[1];
  if(1 != read(fd, buf, 1)) {
    epicLog(LOG_WARNING, "read pipe failed (%d:%s)", errno, strerror(errno));
  }
  epicLog(LOG_DEBUG, "receive local request %c", buf[0]);

  Worker* w = (Worker*)data;
  WorkRequest* wr;
  int i = 0;
  while(w->wqueue->pop(wr)) {
    i++;
    epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
        wr->op, wr->flag, wr->addr, wr->size, wr->fd);
    w->FarmProcessLocalRequest(wr);
  }
  if(!i) epicLog(LOG_DEBUG, "pop %d from work queue", i);
}

Worker::Worker(const Conf& conf, RdmaResource* res):
  st(), wr_psn(),
  wqueue(new boost::lockfree::queue<WorkRequest*>(INIT_WORKQ_SIZE))
{
  epicAssert(wqueue->is_lock_free());
  this->conf = &conf;

  //get the RDMA resource
  if(res) {
    resource = res;
  } else {
    resource = RdmaResourceFactory::getWorkerRdmaResource();
  }

  //create the event loop
  el = aeCreateEventLoop(conf.maxthreads+conf.maxclients+EVENTLOOP_FDSET_INCR);

  //open the socket for listening to the connections from workers to exch rdma resouces
  char neterr[ANET_ERR_LEN];
  char* bind_addr = conf.worker_bindaddr.length() == 0 ? nullptr : const_cast<char *>(conf.worker_bindaddr.c_str());
  sockfd = anetTcpServer(neterr, conf.worker_port, bind_addr, conf.backlog);
  if (sockfd < 0) {
    epicLog(LOG_WARNING, "Opening port %d (bind_addr %s): %s", conf.worker_port, bind_addr, neterr);
    exit(1);
  }

  //register tcp event for rdma parameter exchange
  if (sockfd > 0 && aeCreateFileEvent(el, sockfd, AE_READABLE, AcceptTcpClientHandle, this) == AE_ERR) {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }

  //register rdma event
  if (resource->GetChannelFd() > 0
      && aeCreateFileEvent(el, resource->GetChannelFd(), AE_READABLE, ProcessRdmaRequestHandle, this) == AE_ERR) {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }

  //register the local memory space used for allocation

  void* addr = sb.slabs_init(conf.size, conf.factor, true);
  epicAssert((ptr_t)addr == TOBLOCK(addr));
  RegisterMemory(addr, conf.size);

  //connect to the master
  master = this->NewClient(true);
  master->ExchConnParam(conf.master_ip.c_str(), conf.master_port, this);
  SyncMaster(); //send the local stats to master
  SyncMaster(FETCH_MEM_STATS); //fetch the mem states of other workers from master

#ifdef USE_LOCAL_TIME_EVENT
  if(aeCreateTimeEvent(el, conf.timeout, LocalRequestChecker, this, NULL)) {
    epicPanic("Unrecoverable error creating time event.");
  }
#endif

  epicLog(LOG_INFO, "worker %d started\n", GetWorkerId());
  epicLog(LOG_WARNING, "LRU eviction is enabled, max cache lines = %d, "
      "reserved mem size = %ld, cache_percentage = %f, block_size = %d",
      (int)(conf.size*conf.cache_th/BLOCK_SIZE), conf.size, conf.cache_th, BLOCK_SIZE);
  //create the Master thread to start service
#if defined(USE_BOOST_QUEUE) || defined(USE_BUF_ONLY)
  this->st = new thread(StartService, this);
#else
  this->st = new thread(startEventLoop, el);
#endif
}

void Worker::StartService(Worker* w) {
  aeEventLoop *eventLoop = w->el;
  //start epoll
  eventLoop->stop = 0;
  WorkRequest* wr;
  while (likely(!eventLoop->stop)) {
    if (eventLoop->beforesleep != NULL)
      eventLoop->beforesleep(eventLoop);
    aeProcessEvents(eventLoop, AE_ALL_EVENTS | AE_DONT_WAIT);
#ifdef USE_BOOST_QUEUE
    while(w->wqueue->pop(wr)) {
      epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
          wr->op, wr->flag, wr->addr, wr->size, wr->fd);
      w->ProcessLocalRequest(wr);
    }
#elif defined(USE_BUF_ONLY)
    for(volatile int* buf: w->nbufs) {
      if(*buf == 1) {
        wr = *(WorkRequest**)(buf+1);
        epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
            wr->op, wr->flag, wr->addr, wr->size, wr->fd);
        if(wr->flag & ASYNC) {
          *buf = 2; //notify the app thread to return immediately before we process the request
        } else {
          *buf = 0;
        }
        w->FarmProcessLocalRequest(wr);
      }
    }
#else
    for(volatile int* buf: w->nbufs) {
      if(*buf == 1) {
        while(w->wqueue->pop(wr)) {
          epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
              wr->op, wr->flag, wr->addr, wr->size, wr->fd);
          w->FarmProcessLocalRequest(wr);
        }
      }
    }
#endif
  }

  //end the service
  aeDeleteEventLoop(w->el);
}

/*
 * we use the socket to get the existing workers
 * - guarantee a consistent join sequence of workers
 * - avoid duplicate connection initiations for the same bi-connection
 *
 */
int Worker::PostConnectMaster(int fd, void* data) {
  char inmsg[MAX_WORKERS_STRLEN+1];
  char outmsg[MAX_IPPORT_STRLEN+1];

  epicLog(LOG_DEBUG, "waiting for master reply with worker list");

  /* waiting for server's response */
  int n = read(fd, inmsg, MAX_WORKERS_STRLEN);
  if (n <= 0) {
    epicLog(LOG_WARNING, "Failed to read worker ip/ports (%s)\n", strerror(errno));
    return -1;
  }
  inmsg[n] = '\0';
  epicLog(LOG_DEBUG, "inmsg = %s (n = %d, MAX_WORKERS_STRLEN = %d)", inmsg, n, MAX_WORKERS_STRLEN); 

  n = sprintf(outmsg, "%s:%d", this->GetIP().c_str(), this->GetPort());
  if(n != write(fd, outmsg, n)) {
    epicLog(LOG_WARNING, "send worker ip/port failed (%s)\n", strerror(errno));
    return -2;
  }
  epicLog(LOG_DEBUG, "send: %s; received: %s\n", outmsg, inmsg);

  vector<string> splits;
  Split(inmsg, splits, ',');
  for(string s: splits) {
    if(s.length() < 9) continue;
    vector<string> ip_port;
    Split(s, ip_port, ':');
    epicLog(LOG_INFO, "ip_port = %s", s.c_str());
    epicAssert(ip_port.size() == 2);

    Client* c = this->NewClient();
    c->ExchConnParam(ip_port[0].c_str(), atoi(ip_port[1].c_str()), this);
  }
  return 0;
}

void Worker::RegisterMemory(void* addr, Size s) {
  base = addr;
  size = s;
  resource->RegLocalMemory(addr, s);
}

int Worker::RegisterHandle(int fd, aeFileProc* handle) {
  //register local request event
  if (fd > 0 && aeCreateFileEvent(el, fd, AE_READABLE, ProcessLocalRequest, this) == AE_ERR) {
    epicPanic("Unrecoverable error creating pipe file event.");
    return -1;
  }
  return 0;
}

#ifndef USE_BOOST_QUEUE
int Worker::RegisterNotifyBuf(volatile int* notify_buf) {
  //register local buf
  nbufs.push_back(notify_buf);
  return 0;
}

void Worker::DeRegisterNotifyBuf(volatile int* notify_buf) {
  //register local buf
  nbufs.remove(notify_buf);
}
#endif

void Worker::DeRegisterHandle(int fd) {
  aeDeleteFileEvent(el, fd, AE_READABLE);
}

int Worker::LocalRequestChecker(struct aeEventLoop *eventLoop, long long id, void *clientData) {
  Worker* w = (Worker*)clientData;
  WorkRequest* wr;
  int i = 0;
  while(w->wqueue->pop(wr)) {
    i++;
    epicLog(LOG_DEBUG, "wr->code = %d, wr->flag = %d, wr->addr = %lx, wr->size = %d, wr->fd = %d\n",
        wr->op, wr->flag, wr->addr, wr->size, wr->fd);
    w->FarmProcessLocalRequest(wr);
  }
  if(i) epicLog(LOG_DEBUG, "pop %d from work queue", i);
  return w->conf->timeout;
}

void Worker::SyncMaster(Work op, WorkRequest* parent) {
  WorkRequest* wr = new WorkRequest();
  wr->parent = parent;
  int ret;

  char buf[MAX_REQUEST_SIZE];
  int len = 0;
  wr->op = op;
  if(UPDATE_MEM_STATS == op) {
    wr->size = conf->size;
    wr->free = sb.get_avail();
    ret = FarmSubmitRequest(master, wr);

    //TODO: whether needs to do it in a callback func?
    ghost_size = 0;
  } else if(FETCH_MEM_STATS == op) {
    ret = FarmSubmitRequest(master, wr);
  } else {
    epicLog(LOG_WARNING, "unrecognized sync master op");
    exit(-1);
  }

  epicAssert(ret == 1);
  delete wr;
}

Client* Worker::GetClient(GAddr addr) {
  Client* cli = nullptr;
  int wid = 0;
  UpdateWidMap();
  if(widCliMap.size() == 0) {
    epicLog(LOG_WARNING, "#remote workers is 0!");
  } else {
    if(addr) {
      epicAssert(!IsLocal(addr));
      wid = WID(addr);
    } else {
      //epicLog(LOG_DEBUG, "select a random server to allocate");
      //while ((wid = rand() % widCliMap.size() + 1) == GetWorkerId());
      epicLog(LOG_DEBUG, "select the server with most free memory to allocate");
      Size max = 0;
      for(auto& entry: widCliMap) {
        epicLog(LOG_DEBUG, "worker %d, have %ld free out of %ld",
            entry.second->GetWorkerId(), entry.second->GetFreeMem(), entry.second->GetTotalMem());
        if(max < entry.second->GetFreeMem()) {
          max = entry.second->GetFreeMem();
          wid = entry.first;
        }
      }
    }
    cli = FindClientWid(wid);
    if(!cli) {
      epicLog(LOG_WARNING, "cannot find the client for addr (%d:%lx)", wid, OFF(addr));
    }
  }
  return cli;
}

int Worker::Notify(WorkRequest* wr) {
  if(!(wr->flag & ASYNC)) {
#ifdef USE_PIPE_W_TO_H
    if(write(wr->fd, "r", 1) != 1) {
      epicLog(LOG_WARNING, "writing to pipe error (%d:%s)", errno, strerror(errno));
      return -1;
    }
#elif defined(USE_PTHREAD_COND)
    pthread_mutex_lock(wr->cond_lock);
    pthread_cond_signal(wr->cond);
    pthread_mutex_unlock(wr->cond_lock);
#else
#ifdef USE_BUF_ONLY
    epicAssert(*wr->notify_buf == 0);
#else
    epicAssert(__atomic_load_n(wr->notify_buf, __ATOMIC_RELAXED) == 1);
#endif
    __atomic_store_n(wr->notify_buf, 2, __ATOMIC_RELEASE);
    epicLog(LOG_DEBUG, "writing to notify_buf");
#endif
  }

  return 0;
}

Worker::~Worker() {
  aeDeleteEventLoop(el);
  delete wqueue;
  delete st;
}

int Worker::FarmSubmitRequest(Client* cli, WorkRequest* wr) { 

  char* sbuf = cli->GetFreeSlot();
  if(unlikely(sbuf == nullptr)) {
    // resouce unavailable; need to wait
    return -1;
  }

  char buf[MAX_REQUEST_SIZE];
  if (wr->op == FARM_READ_REPLY) {
    epicAssert(IsLocal(wr->addr));
    char* local = (char*)ToLocal(wr->addr);
    version_t before, after;
    osize_t size;  
    int len = 0;

    // lock-free read
    after = __atomic_load_n((version_t*)local, __ATOMIC_ACQUIRE);
    do {
      before = after;
      while(is_version_wlocked(before))
        before = __atomic_load_n((version_t*)local, __ATOMIC_ACQUIRE);
      runlock_version(&before);
      readInteger(local + sizeof(before), size);
      len = appendInteger(buf, before, size);
      memcpy(buf + len, local + len, size);
      after = __atomic_load_n((version_t*)local, __ATOMIC_ACQUIRE);
    } while (is_version_diff(before, after));

    if (unlikely(before == 0 || size == -1)) {
      epicLog(LOG_INFO, "Address %lx is not allocated or has been free'ed", wr->addr);
      wr->status = Status::READ_ERROR;
    } else {
      wr->status = Status::SUCCESS;
      wr->size = sizeof(before) + sizeof(size) + size;
      epicAssert(wr->size <= MAX_REQUEST_SIZE);
      wr->ptr = buf;
    }
  }

  int len;
  wr->Ser(sbuf, len); 
  int finished = 1;

  if (wr->op == PREPARE) {
    uint16_t cid = cli->GetWorkerId();
    len += local_txns_[wr->id]->generatePrepareMsg(cid,
        sbuf + len, MAX_REQUEST_SIZE - len,
        tx_status_[wr->id]->progress_[cid]);

    if (local_txns_[wr->id]->getNumWobjForWid(cid) > tx_status_[wr->id]->progress_[cid]) {
      finished = 0;
    }

    epicLog(LOG_DEBUG, "wr->nobj = %d, nobj = %d, processed = %d", 
        wr->nobj, 
        local_txns_[wr->id]->getNumWobjForWid(cid), 
        tx_status_[wr->id]->progress_[cid]);

  } else if (wr->op == VALIDATE) {
    uint16_t cid = cli->GetWorkerId();
    len += local_txns_[wr->id]->generateValidateMsg(cid,
        sbuf + len, MAX_REQUEST_SIZE - len,
        tx_status_[wr->id]->progress_[cid]);

    if (local_txns_[wr->id]->getNumRobjForWid(cid) > tx_status_[wr->id]->progress_[cid]) {
      finished = 0;
    }

    epicLog(LOG_DEBUG, "wr->nobj = %d, nobj = %d, processed = %d",
        wr->nobj, 
        local_txns_[wr->id]->getNumRobjForWid(cid), 
        tx_status_[wr->id]->progress_[cid]);
  } else if (wr->op == ACKNOWLEDGE) {
    uint64_t txn_id = cli->GetWorkerId();
    txn_id = (txn_id<<32) | wr->id;
    epicLog(LOG_DEBUG, "finalize for txn %lx", txn_id);
    remote_txns_.erase(txn_id);
    nobj_processed.erase(txn_id);
  }

  int ret = cli->Send(sbuf, len);

  epicLog(LOG_DEBUG, "Worker %d sends a %d:%s msg with wr_id %d, size %d, to Worker %d", 
      this->GetWorkerId(), wr->op, workToStr(wr->op), wr->id, len, cli->GetWorkerId());
  epicAssert(ret == len);

  if (finished)
    return 1;
  else
    return 0;
}

void Worker::FarmAddTask(Client* c, TxnContext* tx) {
  client_tasks_[c].push_back(tx);
  if (client_tasks_[c].size() == 1)
    FarmResumeTxn(c);
}

void Worker::FarmAllocateTxnId(WorkRequest *wr) {
  if (wr->id == -1) {
    // a new transcation arrives
    epicAssert( tx_status_.size() == wr_psn);
    epicAssert( local_txns_.size() == wr_psn);
    local_txns_[wr_psn] = wr->tx;
    tx_status_[wr_psn] = std::move(std::unique_ptr<TxnCommitStatus>(new TxnCommitStatus));
    wr->id = wr_psn++;
  }
}

void Worker::FarmProcessLocalRequest(WorkRequest *wr) {

  FarmAllocateTxnId(wr);
  switch(wr->op) {
    /* no local REPLY messages, as we NOTIFY the caller directly */
    case FARM_MALLOC:
      this->FarmProcessLocalMalloc(wr);
      break;
    case FARM_READ:
      this->FarmProcessLocalRead(wr);
      break;
    case COMMIT:
      this->FarmProcessLocalCommit(wr);
      break;
    case PUT:
    case GET:
      FarmAddTask(master, local_txns_[wr->id]);
      break;
    case KV_PUT:
    case KV_GET:
      {
        epicAssert(wr->counter);
        if(wr->counter == GetWorkerId()) {
          if (wr->op == KV_PUT) {
            void* ptr = zmalloc(wr->size);
            memcpy(ptr, wr->ptr, wr->size);
            kvs[wr->key] = pair<void*, Size>(ptr, wr->size);
            wr->status = SUCCESS;
          } else {
            if(kvs.count(wr->key)) {
              wr->size = kvs.at(wr->key).second;
              memcpy(wr->ptr, kvs.at(wr->key).first, wr->size);
              wr->status = SUCCESS;
            } else {
              wr->status = NOT_EXIST;
              epicLog(LOG_INFO, "not exist locally");
            }
          }
          Notify(wr);
        } else {
          Client* cli = FindClientWid(wr->counter);
          epicAssert(cli);
          FarmAddTask(cli, local_txns_[wr->id]);
        }
        break;
      }
    default:
      epicLog(LOG_WARNING, "Unknown op code %d", wr->op);
      break;
  }
}

void Worker::FarmProcessRemoteRequest(Client* c, const char* msg, uint32_t size) {

  uint32_t wr_id;
  wtype wt;

  int len;

  readInteger((char*) msg, wt, wr_id);

  Work op = static_cast<Work>(wt);

  epicLog(LOG_DEBUG, "Worker %d receives a %s message (wr_id %d, size %d bytes) from Worker %d", 
      this->GetWorkerId(), workToStr(op), wr_id, size, c->GetWorkerId());

  TxnContext *tx;

  if (op == FETCH_MEM_STATS_REPLY || op == BROADCAST_MEM_STATS) {
    // adapted from GAM
    WorkRequest wr;
    wr.Deser(msg, len);

    int wid;
    Size mtotal, mfree;
    vector<Size> stats;
    Split<Size>((char*)wr.ptr, stats);
    epicLog(LOG_DEBUG, "nr_nodes = %d, stats.size = %d", wr.size, stats.size());
    epicAssert(stats.size() == wr.size*3);
    for(int i = 0; i < wr.size; i++) {
      wid = stats[i*3];
      mtotal = stats[i*3+1];
      mfree =stats[i*3+2];
      if(GetWorkerId() == wid) {
        epicLog(LOG_DEBUG, "Ignore self information");
        continue;
      }
      Client* cli = FindClientWid(wid);
      if(cli) {
        cli->SetMemStat(mtotal, mfree);
      } else {
        epicLog(LOG_WARNING, "worker %d not registered yet", wid);
      }
    }
    return;
  }

  if (op & Work::REPLY) {
    // this is a local transaction
    tx = local_txns_.at(wr_id);
  } else {
    // this is a remote transaction
    uint64_t txn_id = ((uint64_t) c->GetWorkerId() << 32 | wr_id);

    // this is a new transaction
    if (this->remote_txns_.count(txn_id) == 0) {
      this->remote_txns_[txn_id] = 
        std::unique_ptr<TxnContext>(new TxnContext);
      this->nobj_processed[txn_id] = 0;
    }

    tx = remote_txns_[txn_id].get();
  }

  WorkRequest* wr = tx->wr_;
  wr->Deser(msg, len);
  if (len < size) {
    wr->size = (size - len);
    wr->ptr = (void*)(msg + len);
  }

  switch(wr->op) {
    case FARM_MALLOC:
      this->FarmProcessMalloc(c, tx);
      break;
    case FARM_MALLOC_REPLY:
      this->FarmProcessMallocReply(c, tx);
      break;
    case FARM_READ:
      this->FarmProcessRead(c, tx);
      break;
    case FARM_READ_REPLY:
      this->FarmProcessReadReply(c, tx);
      break;
    case PREPARE:
      this->FarmProcessPrepare(c, tx);
      break;
    case VALIDATE:
      this->FarmProcessValidate(c, tx);
      break;
    case PREPARE_REPLY:
      this->FarmProcessPrepareReply(c, tx);
      break;
    case VALIDATE_REPLY:
      this->FarmProcessValidateReply(c, tx);
      break;
    case COMMIT:
      this->FarmProcessCommit(c, tx);
      break;
    case ABORT:
      this->FarmProcessAbort(c, tx);
      break;
    case ACKNOWLEDGE:
      this->FarmProcessAcknowledge(c, tx);
      break;
    case GET_REPLY:
    case PUT_REPLY:
      Notify(tx->wr_);
      break;
    case KV_PUT:
      {
        void* ptr = zmalloc(wr->size);
        memcpy(ptr, wr->ptr, wr->size);
        kvs[wr->key] = pair<void*, Size>(ptr, wr->size);


        // send reply back
        wr->op = PUT_REPLY;
        wr->status = SUCCESS;
        FarmAddTask(c, tx);
        break;
      }
    case KV_GET:
      {
        if(kvs.count(wr->key)) {
          wr->ptr = kvs.at(wr->key).first;
          wr->size = kvs.at(wr->key).second;
          wr->op = GET_REPLY;
          wr->status = SUCCESS;
        } else {
          wr->op = GET_REPLY;
          wr->status = NOT_EXIST;
          epicLog(LOG_INFO, "not exist remotely");
        }
        FarmAddTask(c, tx);
        break;
      }
    default:
      epicLog(LOG_WARNING, "Unknown op code %d", tx->wr_->op);
      break;
  }
}

/**
 * @brief process malloc request issued by local application threads
 *
 * @param tx
 */
void Worker::FarmProcessLocalMalloc(WorkRequest *wr) {
  epicAssert(wr->op == FARM_MALLOC);
  TxnContext* tx = local_txns_[wr->id];

  bool remote = true;
  if (!wr->addr || IsLocal(wr->addr)) {
    /* local malloc */
    void *addr;
    if (wr->flag & ALIGNED) 
      addr = FarmMalloc(wr->size, true);
    else
      addr = FarmMalloc(wr->size);

    if (likely(addr)) {
      memset(addr, 0, wr->size); //ensure it is not locked
      wr->addr = TO_GLOB(addr, base, GetWorkerId());
      remote = false;
      wr->status = SUCCESS;
      this->ghost_size += wr->size;
      wr->op = FARM_MALLOC_REPLY;
      if (ghost_size > conf->ghost_th) SyncMaster();
    } else {
      wr->addr = Gnullptr;
    }
  }

  if (remote) {
    /* remote allocation */
    Client *cli = GetClient(wr->addr);
    if (likely(cli)) {
      FarmAddTask(cli, tx);
      return;
    } else {
      wr->status = ALLOC_ERROR;
    }
  }

  if(Notify(wr)) {
    epicLog(LOG_WARNING, "cannot wake up the app thread");
  }
}

/**
 * @brief process a malloc request from Client @param cli
 *
 * @param cli
 * @param wr
 *
 * @return 1 if this wr has been sent back to client
 */
void Worker::FarmProcessMalloc(Client* c, TxnContext* tx) {
  WorkRequest* wr = tx->wr_;

  epicLog(LOG_DEBUG, "Worker %d receives a local %s msg", GetWorkerId(), workToStr(wr->op));
  wr->op = FARM_MALLOC_REPLY;

  void *addr;
  if (wr->flag & ALIGNED) {
    addr = sb.sb_aligned_malloc(wr->size);
  } else {
    addr = sb.sb_malloc(wr->size);
  }

  if (likely(addr)) {
    memset(addr, 0, wr->size); //ensure it is not locked
    wr->status = SUCCESS;
    wr->addr = TO_GLOB(addr, base, GetWorkerId());
    ghost_size += wr->size;
  } else {
    wr->status = ALLOC_ERROR;
  }

  FarmAddTask(c, tx);

  //TODO: adapt to Farm
  if (ghost_size > conf->ghost_th) SyncMaster();
}

void Worker::FarmProcessMallocReply(Client* c, TxnContext* tx) {
  // nothing to do but just notify of app thread
  epicAssert (tx->wr_->status == SUCCESS || tx->wr_->status == READ_ERROR);
  Notify(tx->wr_);
}


/**
 * @brief process a reqd request issued by a local application thread
 *
 * @param wr
 */
void Worker::FarmProcessLocalRead(WorkRequest* wr) {
  epicLog(LOG_DEBUG, "Worker %d receives a local %s msg for %lx", GetWorkerId(), workToStr(wr->op), wr->addr);

  TxnContext* tx = this->local_txns_[wr->id];

  epicAssert(!IsLocal(wr->addr));
  //if (IsLocal(wr->addr))
  if (0) {
    /* local read */
    wr->op = FARM_READ_REPLY;

    char* base = (char*)ToLocal(wr->addr);
    version_t v;
    osize_t s;
    readInteger(base, v, s);

    if (unlikely(v == 0 || s == -1)) {
      // this address is free'ed or has not written
      wr->status = Status::READ_ERROR;
      epicLog(LOG_DEBUG, "Address %lx is not alloc'ed or has been free'ed", wr->addr);
    } else {
      // allow read while being locked
      if (0) {
        to_serve_local_requests[wr->addr].push(wr);
        return;
      } else {
        Object* o = local_txns_[wr->id]->createReadableObject(wr->addr);
        wr->status = SUCCESS;
        o->deserialize(reinterpret_cast<const char*>(base));
      }
    }
  } else {
    /* remote read: forward request to designated worker */
    Client *c = GetClient(wr->addr);
    if (unlikely(!c)) {
      wr->status = READ_ERROR;
    } else {
      to_serve_local_requests[wr->addr].push(wr);
      if (to_serve_local_requests[wr->addr].size() == 1)
        FarmAddTask(c, local_txns_[wr->id]);
      return;
    }
  }

  if(Notify(wr)) {
    epicLog(LOG_WARNING, "cannot wake up the app thread");
  }
}

/**
 * @brief process a read request from client @param c
 *
 * @param c
 * @param tx
 * @param wr
 *
 * @return 
 */
void Worker::FarmProcessRead(Client* c, TxnContext* tx) {
  WorkRequest* wr = tx->wr_;
  epicAssert(IsLocal(wr->addr));
  wr->op = FARM_READ_REPLY;

  FarmAddTask(c, tx);
}

void Worker::FarmProcessReadReply(Client* c, TxnContext* tx) {
  WorkRequest* wr = tx->wr_;
  epicAssert (wr->status == SUCCESS || wr->status == READ_ERROR);

  //Notify(wr);
  FarmProcessPendingReads(wr);
}


/**
 * @brief perform twp-phase commit on behalf of an application thread
 *
 * @param wr: a work request 
 */
void Worker::FarmProcessLocalCommit(WorkRequest* wr) {
  epicLog(LOG_DEBUG, "Worker %d tries to commit txn", GetWorkerId(), wr->id);
  TxnCommitStatus* ts;

  if (wr->op == COMMIT) {
    // handler the case where id equal to -1
    FarmAllocateTxnId(wr);
    ts = tx_status_[wr->id].get();

    // if op is not COMMIT, then this is performed in app thread;
    // otherwise this is performed in worker thread
    ts->local = false;
  } else {
    ts = new TxnCommitStatus;
    ts->local = true;
  }

  ts->progress_.clear();

  FarmPrepare(wr->tx, ts);
}

/**
 * @brief resume pending transactions that got stuck due to insufficient send
 * slot or locks; This should be called when previous send verbs complete.
 *
 * @param c client with free slots
 */
void Worker::FarmResumeTxn(Client* c) {

  if (this->client_tasks_[c].empty()) return;

  TxnContext* tx = this->client_tasks_[c].front();

  //if (unlikely(c == nullptr)) {
  //    epicAssert(wr->op == FARM_READ);
  //    FarmProcessLocalRead(wr);
  //    return;
  //}

  int ret = FarmSubmitRequest(c, tx->wr_);
  if (ret == 1) {
    // this work request has been completed; 
    // continue to process next one
    client_tasks_[c].pop_front();
  } else {
    if (ret == -1)
      // should not retry if there is no free send slot
      return;
  }

  FarmResumeTxn(c);
}

/**
 * @brief PREPARE phase: first check if each local object
 * contained in the write set is either locked or changed. 
 * If yes, abort the transaction without sending prepare
 * messages to other workers. 
 *
 * @param s
 * @param c
 *
 * @return 
 */

void Worker::FarmPrepare(TxnContext* tx, TxnCommitStatus* ts){

  // mark the operation code for the respective work request
  WorkRequest* wr = tx->wr_;

  epicLog(LOG_DEBUG, "Txn %d enters PREPARE phase", wr->id);

  std::vector<uint16_t> wids;
  uint16_t wid = GetWorkerId();

  tx->getWidForWobj(wids);

  ts->success = 1;
  ts->remaining_workers_ = wids.size();

  for (auto& p: wids) {
    ts->progress_[p] = 0;
  }

  // remote prepare
  for (auto& p: wids) {
    if (p != wid) {
      Client* c = FindClientWid(p);
      if (likely(c)) FarmPrepare(c, tx);
      else {
        wr->status == COMMIT_FAILED;
        return;
      }
    }
  }

  if (wids.empty() || (wids.size() == 1 && wids[0] == wid))
    // local transaction
    FarmResumePrepare(tx, ts);
}



/**
 * @brief resume prepare phase at local worker after recv'ed all prepare
 * replies from remote workers
 *
 * @param tx
 */
void Worker::FarmResumePrepare(TxnContext* tx, TxnCommitStatus* ts) {

  WorkRequest* wr = tx->wr_;

  if (ts == nullptr) {
    epicAssert(wr->id != -1);
    ts = this->tx_status_[wr->id].get();
  }

  epicAssert(ts->remaining_workers_ == 0 || ts->remaining_workers_ == 1);

  int locked = 0;

  if (!ts->success) 
    goto abort;

  // write sets processed in local worker
  if (ts->remaining_workers_ == 1) {

    epicAssert(tx->getNumWobjForWid(GetWorkerId()) > 0);

    std::unordered_map<GAddr, std::shared_ptr<Object>>& wset = tx->getWriteSet(GetWorkerId());
    version_t v;
    osize_t s;
    char* local;
    for (auto& e: wset) {
      local = (char*)(ToLocal(e.first));
      //readInteger(local, v, s);
      readInteger(local+sizeof(v), s);
      if (FarmRLock(e.first)) {
        ++locked;
        if ( s == -1 || FarmAllocSize(local) < e.second->getTotalSize()) 
        {
          //v = __atomic_load_n((version_t*)ToLocal(e.first), __ATOMIC_RELAXED);
          epicLog(LOG_DEBUG, "Address %lx, version = %lx, size = %d, allocated size = %d, objcet size = %d ",
              e.first, e.second->getVersion(),
              s,
              FarmAllocSize(local),
              e.second->getTotalSize());
          goto abort;
        }
      } else {
        epicLog(LOG_DEBUG, "Address %lx has been locked by another txn", e.first);
        goto abort;
      }
    }
  }

  wr->op = VALIDATE;
  wr->status = SUCCESS;
  FarmValidate(tx, ts);
  return;

abort:
  if (ts->remaining_workers_ == 1) {
    std::unordered_map<GAddr, std::shared_ptr<Object>>& wset = tx->getWriteSet(GetWorkerId());
    int i = 0;
    for (auto& p : wset) {
      if (i++ == locked)
        break;
      FarmUnRLock(p.first);
    } 
    wset.clear();
  }
  ts->success = 0;
  wr->op = Work::ABORT;
  FarmCommitOrAbort(tx, ts);
}


/**
 * @brief ask a remote worker to resume prepare phase for the given
 * transaction
 *
 * @param s: resumed transaction
 * @param c: client
 *
 */
void Worker::FarmPrepare(Client* c, TxnContext* tx) {
  uint16_t wid = c->GetWorkerId();

  WorkRequest* wr = tx->wr_;

  TxnCommitStatus* ts = tx_status_[wr->id].get();

  //epicAssert(wr->op == PREPARE || wr->op == PREPARE_REPLY);

  char* msg;
  int nobj, len, offset;

  wr->nobj = tx->getNumWobjForWid(wid);
  wr->op = PREPARE;
  FarmAddTask(c, tx);
}

/**
 * @brief Process a remote prepare message
 *
 * @param msg
 * @param mlen
 */
void Worker::FarmProcessPrepare(Client* c, TxnContext* tx) {
  WorkRequest* wr = tx->wr_;
  char *buf, *msg;
  buf = msg = (char*)wr->ptr;
  int mlen = 0;
  version_t ver;

  //buf += readInteger(buf, nobj);

  wr->op = PREPARE_REPLY;
  GAddr a;
  osize_t s;

  char* local;

  while (mlen < wr->size) {
    mlen += readInteger(msg + mlen, a, s);
    Object* o = tx->createWritableObject(a);
    o->setSize(s);
    //mlen += o->deserialize(msg + mlen, wr->size - mlen);
    mlen += o->readEmPlace(msg + mlen, 0, s);
  }

  std::unordered_map<GAddr, std::shared_ptr<Object>>& wid = 
    tx->getWriteSet(GetWorkerId()); 


  if (wid.size() == wr->nobj) {
    /* recv'ed all writable objects */

    int locked = 0;
    for (auto& p: wid) {
      local = (char*)(ToLocal(p.first));
      readInteger(local + sizeof(ver), s);

      // TODO: check if this address is valid or not
      if (FarmRLock(p.first)) {
        ++locked;
        if ( s == -1 || FarmAllocSize(local) < p.second->getTotalSize()) 
        {
          /* this transaction should be abort;
           * no writable objects are locked by this txn
           */
          epicLog(LOG_INFO, "Address %lx, new version = %d, locked = %d, size= %d, allocated size = %d, objcet size = %d ",
              p.first, p.second->getVersion(), is_version_locked(ver), s, FarmAllocSize(local), p.second->getTotalSize());
          break;
        }
      } else {
        epicLog(LOG_INFO, "Address %lx is locked by another txn");
        break;
      }
    }

    if (locked < wid.size())
    {
      int i = 0;
      wr->status = PREPARE_FAILED;
      for (auto& p : wid) {         
        if (i++ == locked)
          break;
        FarmUnRLock(p.first);
      }
      wid.clear();
    } else {
      wr->status = SUCCESS;
    }

    // we only submit request after the entire prepare message has been
    // recv'ed.
    FarmAddTask(c, tx);
  }
}

/**
 * @brief process the response of a prepare message
 *
 * @param c
 * @param tx
 */
void Worker::FarmProcessPrepareReply(Client* c, TxnContext* tx)   {
  WorkRequest* wr = tx->wr_;

  epicAssert(wr->status == SUCCESS || wr->status == PREPARE_FAILED);

  TxnCommitStatus* ts = tx_status_[wr->id].get();

  ts->remaining_workers_--;

  if (wr->status == PREPARE_FAILED) {
    ts->success = 0;
  }

  // This is necessary as there might be pending prepare messages not yet
  // sent; reset the op back to prepare can ensure the pending messages be
  // sent correctly instead of as a PREPARE_REPLY message.
  wr->op = PREPARE;

  if (0 == ts->remaining_workers_ || 
      (1 == ts->remaining_workers_ && tx->getNumWobjForWid(GetWorkerId()) > 0)) {
    FarmResumePrepare(tx);
  }
}

void Worker::FarmProcessValidateReply(Client* c, TxnContext* tx) {
  WorkRequest* wr = tx->wr_;

  epicAssert(wr->status == SUCCESS || wr->status == VALIDATE_FAILED);

  TxnCommitStatus* ts = tx_status_[wr->id].get();

  if (wr->status == VALIDATE_FAILED) {
    ts->success = 0;
  }

  // This is necessary as there might be pending validate messages not yet
  // sent; reset the op back to validate can ensure the pending messages be
  // sent correctly instead of as a VALIDATE_REPLYmessage.
  wr->op = VALIDATE;

  --ts->remaining_workers_;
  if (0 == ts->remaining_workers_) {
    FarmResumeValidate(tx);
  }
}

void Worker::FarmProcessAcknowledge(Client* c, TxnContext* tx) {
  epicAssert(tx->wr_->status == SUCCESS);
  epicAssert(tx->wr_->op == ACKNOWLEDGE);
  TxnCommitStatus* ts = tx_status_[tx->wr_->id].get();

  // This is to prevent the pending commit/abort messages from being sent as
  // an ack msg. 
  // if (ts->success)
  //     tx->wr_->op = COMMIT;
  // else
  //     tx->wr_->op = ABORT;

  if (0 == --ts->remaining_workers_) {
    FarmFinalizeTxn(tx, ts);
  }
}

/**
 * @brief validate pahse; similar to FarmPrepare
 *
 * @param s
 */
void Worker::FarmValidate(TxnContext* tx, TxnCommitStatus* ts) {

  // mark the operation code for the respective work request
  WorkRequest* wr = tx->wr_;

  epicLog(LOG_DEBUG, "Txn %d enters VALIDATE phase", wr->id);

  std::vector<uint16_t> wids;
  uint16_t wid = GetWorkerId();

  tx->getWidForRobj(wids);
  for (auto& p: wids) {
    ts->progress_[p] = 0;
  }

  ts->remaining_workers_ = wids.size();
  epicAssert(ts->success == 1); 

  if (tx->getNumRobjForWid(wid) > 0) {
    /* local validate*/

    std::unordered_map<GAddr, std::shared_ptr<Object>>& rset = tx->getReadSet(wid);

    version_t v, rv;
    for (auto& e: rset) {

      v = __atomic_load_n((version_t*)ToLocal(e.first), __ATOMIC_RELAXED);

      version_t v1 = e.second->getVersion();
      epicAssert(v1 != 0 && !is_version_locked(v1));

      // if versions do not match or object has been free'ed or locked, abort
      if (is_version_diff(v1, v) || (is_version_rlocked(v) && !tx->containWritable(e.first))) {
        // abort the tx; we cannot immediately return to the
        // application as there may have been some objects locked by
        // this transaction
        epicLog(LOG_INFO, "Fail to validate %lx, rv = %lx, %s", e.first, v1, e.second->toString());
        wr->op = Work::ABORT;
        ts->success = 0;
        FarmCommitOrAbort(tx, ts);
        return;
      }
    }

    ts->remaining_workers_--; 
  }

  if (ts->remaining_workers_ == 0) {
    // only local prepare
    // enter commit phase
    wr->op = COMMIT;
    FarmCommitOrAbort(tx, ts);
    return;
  }

  // remote validate
  for (auto& p: wids) {
    if (p == wid) continue;

    Client* c = FindClientWid(p);
    if(likely(c))
      FarmValidate(c, tx);
  }

}

/**
 * @brief send a validate wr to a client
 *
 * @param c
 * @param wr
 *
 * @return 
 */
void Worker::FarmValidate(Client* c, TxnContext* tx) {

  epicAssert(tx->wr_->op == VALIDATE || tx->wr_->op == VALIDATE_REPLY);

  int nobj = tx->getNumRobjForWid(c->GetWorkerId());

  /* messages are for remote clients */
  tx->wr_->nobj = nobj;

  FarmAddTask(c, tx);
}

/**
 * @brief resume prepare phase at the local coordinator after recv'ing all replies
 *
 * @param tx
 */
void Worker::FarmResumeValidate(TxnContext* tx, TxnCommitStatus* ts) {
  WorkRequest* wr = tx->wr_;
  uint16_t wid = GetWorkerId();

  if (ts == nullptr) {
    // distributed txn
    epicAssert(wr->id != -1);
    ts = this->tx_status_[wr->id].get();
  }

  wr->op = Work::COMMIT;
  if (!ts->success) {
    // unlock the objects locked in the PREPARE phase
    if (tx->getNumWobjForWid(wid) > 0) {
      for (auto& q: tx->getWriteSet(wid)) {
        FarmUnRLock(q.first);
      }
      tx->getWriteSet(wid).clear();
    }
    wr->op = Work::ABORT;
  }

  FarmCommitOrAbort(tx, ts);
}

void Worker::FarmProcessValidate(Client* c, TxnContext* tx) {

  WorkRequest* wr = tx->wr_;
  char *buf, *msg;
  buf = msg = (char*)wr->ptr;
  int mlen = wr->size;

  if (wr->status == VALIDATE_FAILED) {
    // we have already replied
    epicLog(LOG_DEBUG, "This txn has been invalidated");
    return;
  }

  wr->op = VALIDATE_REPLY;
  GAddr a;
  version_t v1, v2;
  uint64_t txn_id = c->GetWorkerId();
  txn_id = (txn_id << 32) | wr->id;

  while (buf < msg + mlen) {
    buf += readInteger(buf, a, v1);
    epicAssert(v1 != 0 && !is_version_locked(v1));

    nobj_processed[txn_id]++;

    v2 = __atomic_load_n((version_t*)ToLocal(a), __ATOMIC_RELAXED);
    //runlock_version(&v2);

    // if versions do not match or object has been free'ed or locked, abort
    if (is_version_diff(v1, v2) || ((is_version_rlocked(v2)) && !tx->containWritable(a))) {
      wr->status = VALIDATE_FAILED;
      epicLog(LOG_INFO, "Fail to validate object %lx: old version = %ld, new version = %ld, rlocked = %d",
          a, v1, v2, is_version_rlocked(v2));
      goto reply;
    }
  }

  //if (wr->nobj < nobj_processed[txn_id])
  //    epicLog(LOG_WARNING, "total= %d, processed = %d", wr->nobj, nobj_processed[txn_id]);

  epicAssert(wr->nobj >= nobj_processed[txn_id]);

  if (wr->nobj > nobj_processed[txn_id]) {
    return;
  }

reply:
  FarmAddTask(c, tx);
  if (tx->getNumWobjForWid(GetWorkerId()) == 0) {
    // Finalize this txn as I won't receive commit/abort
    // messages
    remote_txns_.erase(txn_id);
    nobj_processed.erase(txn_id);
  }
}


/**
 * @brief commit or abort the transaction
 *
 * @param s: transcation status
 */
void Worker::FarmCommitOrAbort(TxnContext* tx, TxnCommitStatus* ts) {

  uint16_t wid = GetWorkerId();
  std::vector<uint16_t> wids;

  WorkRequest* wr = tx->wr_;

  // success indicates whether to commit or abort
  epicAssert((ts->success && wr->op == COMMIT) || (!ts->success && wr->op == ABORT));

  int level = (wr->op == COMMIT) ? LOG_DEBUG: LOG_INFO;
  //epicLog(level, "Txn %d %s", wr->id, workToStr(wr->op));

  tx->getWidForWobj(wids);
  ts->remaining_workers_ = wids.size();

  if (tx->getNumWobjForWid(wid) > 0) {
    std::unordered_map<GAddr, std::shared_ptr<Object>>& wset = 
      tx->getWriteSet(GetWorkerId());

    if (wr->op == COMMIT) {
      FarmWrite(wset);
    } else {
      GAddr a = wset.begin()->first;
      if (FarmAddressRLocked(a) && tx->containWritable(a)) {
        epicAssert(!FarmAddressWLocked(a));
        FarmUnRLock(a);
        wset.erase(a);
        for (auto& p: wset) {
          epicAssert(!FarmAddressWLocked(p.first) && FarmAddressRLocked(p.first));
          FarmUnRLock(p.first);
        }
      }
    }

    --ts->remaining_workers_;
  }

  if (ts->remaining_workers_ == 0) {
    // txn completed
    FarmFinalizeTxn(tx, ts);
    return;
  }

  // remote commit 
  for (auto& p: wids) {
    if (p == wid) continue;
    Client* c = FindClientWid(p);
    if(likely(c))
      FarmCommitOrAbort(c, tx);
  }
}

/**
 * @brief ask a remote worker to commit or abort the given txn
 *
 * @param s
 * @param c
 */
void Worker::FarmCommitOrAbort(Client* c, TxnContext* tx) {
  epicAssert(tx->wr_->op == COMMIT || tx->wr_->op == ABORT);

  FarmAddTask(c, tx);
}

void Worker::FarmProcessAbort(Client* c, TxnContext* tx) {

  std::unordered_map<GAddr, std::shared_ptr<Object>>& wset = tx->getWriteSet(GetWorkerId());

  for (auto& p: wset) 
    FarmUnRLock(p.first);

  FarmFinalizeTxn(c, tx);
}

void Worker::FarmProcessCommit(Client* c, TxnContext* tx) {

  epicAssert(tx);

  FarmWrite(tx->getWriteSet(GetWorkerId()));

  FarmFinalizeTxn(c, tx);
}

void Worker::FarmWrite(std::unordered_map<GAddr, std::shared_ptr<Object>>& wset) {
  // first wlock
  bool ret;
  for (auto& p: wset) {
    epicAssert(FarmAddressRLocked(p.first));
    ret = FarmWLock(p.first);
    epicAssert(ret);
  }

  // memory barrier to ensure objects are locked while being updated.
  __sync_synchronize();

  for (auto& p: wset) {
    Object *o = p.second.get();
    char* local = (char*)ToLocal(o->getAddr()) + sizeof(version_t);
    local += appendInteger(local, o->getSize());
    if (o->getSize() >= 0) {
      o->writeTo(local, 0, o->getSize());
      FarmUnWLock(o->getAddr());
      epicLog(LOG_DEBUG, "Serialize %lx: %s", o->getAddr(), o->toString());
    } else {
      FarmUnWLock(o->getAddr());
      FarmFree(o->getAddr());
      epicLog(LOG_DEBUG, "Free Address %lx", o->getAddr());
    }
  }

  // TODO: logging goes here
}

/**
 * @brief clear resource for a local transaction; and resume pending reads
 * against the objects just unlocked.
 *
 * @param s
 */
void Worker::FarmFinalizeTxn(TxnContext* tx, TxnCommitStatus* ts) {

  if (ts->success)
    tx->wr_->status = Status::SUCCESS;
  else
    tx->wr_->status = Status::COMMIT_FAILED;

  // there cannot be reads pending on the write set
  // FarmProcessPendingReads(tx);

  if (!ts->local)
    Notify(tx->wr_);
  else
    delete ts;
}

/**
 * @brief finalize a rmeote transaction
 *
 * @param s
 */
void Worker::FarmFinalizeTxn(Client* c, TxnContext* tx) {
  WorkRequest* wr = tx->wr_;
  uint64_t txn_id = c->GetWorkerId();
  txn_id = (txn_id << 32) | wr->id;

  // resume pending read
  FarmProcessPendingReads(remote_txns_[txn_id].get());

  wr->op = Work::ACKNOWLEDGE;
  wr->status = Status::SUCCESS;
  FarmAddTask(c, tx);

  // we can only clean the txn context when ack message has been sent
  // delete this remote transaction
  // epicLog(LOG_DEBUG, "finalize for txn %lx", txn_id);
  // remote_txns_.erase(txn_id);
  // nobj_processed.erase(txn_id);
}

void Worker::FarmProcessPendingReads(WorkRequest* wr) {
  epicAssert(!IsLocal(wr->addr));

  queue<WorkRequest*>& q = to_serve_local_requests[wr->addr];
  WorkRequest* twr;
  while(!q.empty()) {
    twr = q.front();
    twr->op = FARM_READ_REPLY;

    if (twr == wr) {
      q.pop();
      continue;
    }

    twr->status = wr->status;
    if (wr->status == SUCCESS) {
      local_txns_[twr->id]->createReadableObject(twr->addr)->deserialize((char*)wr->ptr);
    }

    Notify(twr);
    q.pop();
  }

  //make sure wr is lastly notified; otherwise its ptr may be changed before
  //other pending wrs read it
  if (wr->status == SUCCESS) {
    local_txns_[wr->id]->createReadableObject(wr->addr)->deserialize((char*)wr->ptr);
  }
  Notify(wr);
}

void Worker::FarmProcessPendingReads(TxnContext* tx) {

  version_t v;
  osize_t o;
  int len;
  TxnContext* t;
  WorkRequest* wr;

  if (tx->getNumWobjForWid(GetWorkerId()) <= 0)
    return;

  std::unordered_map<GAddr, std::shared_ptr<Object>>& wset = tx->getWriteSet(GetWorkerId());
  for (auto& a: wset) {
    if (IsLocal(a.first)) {
      if (unlikely(to_serve_local_requests.count(a.first) > 0))
        FarmProcessPendingReads(to_serve_local_requests[a.first].front());
    } else {
      if (likely(to_serve_requests.count(a.first) == 0)) continue;

      queue<pair<Client*, WorkRequest*>>& q = to_serve_requests[a.first];
      while(!q.empty()) {
        WorkRequest* wr = q.front().second;
        Client* c = q.front().first;
        epicAssert(wr->op == FARM_READ_REPLY);
        uint64_t tid = c->GetWorkerId();
        tid = (tid << 32) | wr->id;

        // put this work request into the ready queue
        FarmAddTask(c, remote_txns_[tid].get());
        q.pop();
      }
    }
  }
}

void Worker::FarmFree(GAddr addr) {
  char* local = (char*)ToLocal(addr);
  uint8_t offset = 0;
  local -= sizeof(uint8_t) + sizeof(osize_t);
  readInteger(local, offset);
  sb.sb_free(local - offset);
}

void* Worker::FarmMalloc(osize_t size, osize_t align) {
  char* addr;
  int sbits = sizeof(osize_t);
  int vbits = sizeof(version_t);
  int obits = sizeof(uint8_t);
  uint8_t offset;
  osize_t rsize = size + sbits + vbits; 

  if (align > 0) {
    addr = (char*)sb.sb_aligned_malloc(rsize, align);
  }
  else
    addr = (char*)sb.sb_aligned_malloc(rsize);

  if (likely(addr)) {
    uintptr_t p = (uintptr_t)addr + sbits + obits;
    offset = (vbits - p % vbits) % vbits;
    addr += offset;
    epicAssert(offset + obits <= vbits);
    size += (vbits - offset - obits);
    addr += appendInteger(addr, offset, size);
    memset(addr, 0, size);
    epicAssert((uintptr_t)addr % vbits == 0);
    //__atomic_store_n((version_t*)addr, 0, __ATOMIC_RELAXED);
  }

  return addr;
}

inline bool Worker::FarmRLock(GAddr addr) {
  epicLog(LOG_DEBUG, "rlock object %lx", addr);
  return rlock_object(ToLocal(addr));
}

inline void Worker::FarmUnRLock(GAddr addr) {
  epicLog(LOG_DEBUG, "read unlock object %lx", addr);
  runlock_object(ToLocal(addr));
}

inline bool Worker::FarmWLock(GAddr addr) {
  epicLog(LOG_DEBUG, "wlock object %lx", addr);
  return wlock_object(ToLocal(addr));
}

inline void Worker::FarmUnWLock(GAddr addr) {
  epicLog(LOG_DEBUG, "write unlock object %lx", addr);
  wunlock_object(ToLocal(addr));
}

inline bool Worker::FarmAddressLocked(GAddr addr) {
  return is_object_locked(ToLocal(addr));
}

inline bool Worker::FarmAddressRLocked(GAddr addr) {
  return is_object_rlocked(ToLocal(addr));
}

inline bool Worker::FarmAddressWLocked(GAddr addr) {
  return is_object_wlocked(ToLocal(addr));
}


