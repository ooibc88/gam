// Copyright (c) 2018 The GAM Authors 


#include <thread>
#include <unistd.h>
#include <cstring>
#include "zmalloc.h"
#include "rdma.h"
#include "master.h"
#include "anet.h"
#include "log.h"
#include "ae.h"
#include "tcp.h"
#include "settings.h"

Master* MasterFactory::server = nullptr;

Master::Master(const Conf& conf)
    : st(nullptr),
      workers(),
      unsynced_workers() {

  this->conf = &conf;

  //get the RDMA resource
  resource = RdmaResourceFactory::getMasterRdmaResource();

  //create the event loop
  el = aeCreateEventLoop(
      conf.maxthreads + conf.maxclients + EVENTLOOP_FDSET_INCR);

  //open the socket for listening to the connections from workers to exch rdma resouces
  char neterr[ANET_ERR_LEN];
  char* bind_addr =
      conf.master_bindaddr.length() == 0 ?
          nullptr : const_cast<char*>(conf.master_bindaddr.c_str());
  sockfd = anetTcpServer(neterr, conf.master_port, bind_addr, conf.backlog);
  if (sockfd < 0) {
    epicLog(LOG_WARNING, "Opening port %s:%d (%s)",
            conf.master_bindaddr.c_str(), conf.master_port, neterr);
    exit(1);
  }

  //register tcp event for rdma parameter exchange
  if (sockfd
      > 0&& aeCreateFileEvent(el, sockfd, AE_READABLE, AcceptTcpClientHandle, this) == AE_ERR) {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }

  //register rdma event
  if (resource->GetChannelFd()
      > 0 && aeCreateFileEvent(el, resource->GetChannelFd(), AE_READABLE, ProcessRdmaRequestHandle, this) == AE_ERR) {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }

  epicLog(LOG_INFO, "start master eventloop\n");
  //create the Master thread to start service
  this->st = new thread(startEventLoop, el);
}

int Master::PostAcceptWorker(int fd, void* data) {
  if (worker_ips.length() == 0) {
    if (1 != write(fd, " ", 1)) {
      epicLog(LOG_WARNING, "Unable to send worker ip list\n");
      return -1;
    }
  } else {
    if (worker_ips.length()
        != write(fd, worker_ips.c_str(), worker_ips.length())) {
      epicLog(LOG_WARNING, "Unable to send worker ip list\n");
      return -1;
    }
    epicLog(LOG_DEBUG, "send: %s", worker_ips.c_str());

    worker_ips.append(",");
  }

  char msg[MAX_WORKERS_STRLEN + 1];
  int n = read(fd, msg, MAX_WORKERS_STRLEN);
  if (n <= 0) {
    epicLog(LOG_WARNING, "Unable to receive worker ip:port\n");
    return -2;
  }
  worker_ips.append(msg, n);
  msg[n] = '\0';

  epicLog(LOG_DEBUG, "received: %s, now worker list is %s (len=%d)\n", msg,
          worker_ips.c_str(), worker_ips.length());
  return 0;
}

Master::~Master() {
  aeDeleteEventLoop(el);
  delete st;
  st = nullptr;
}

void Master::ProcessRequest(Client* client, WorkRequest* wr) {
  epicAssert(wr->wid == client->GetWorkerId());
  switch (wr->op) {
    case UPDATE_MEM_STATS: {
      epicAssert(wr->size);
      Size curr_free = client->GetFreeMem();
      client->SetMemStat(wr->size, wr->free);
      unsynced_workers.push(client);

      widCliMapWorker[client->GetWorkerId()] = client;

      if (unsynced_workers.size() == conf->unsynced_th) {
        WorkRequest lwr { };
        lwr.op = BROADCAST_MEM_STATS;
        char buf[conf->unsynced_th * MAX_MEM_STATS_SIZE + 1];  //op + list + \0
        char send_buf[MAX_REQUEST_SIZE];

        int n = 0;
        while (!unsynced_workers.empty()) {
          Client* lc = unsynced_workers.front();
          n += sprintf(buf + n, "%u:%d:%ld:%ld", lc->GetQP(), lc->GetWorkerId(),
                       lc->GetTotalMem(), lc->GetFreeMem());
          unsynced_workers.pop();
        }
        lwr.size = conf->unsynced_th;
        lwr.ptr = buf;
        int len = 0;
        lwr.Ser(send_buf, len);
        Broadcast(send_buf, len);
      }
      delete wr;
      wr = nullptr;
      break;
    }
    case FETCH_MEM_STATS: {
      //UpdateWidMap();
      epicAssert(widCliMap.size() == qpCliMap.size());
//		if(widCliMapWorker.size() == 1) { //only have the info of the worker, who sends the request
//			break;
//		}
      WorkRequest lwr { };
      lwr.op = FETCH_MEM_STATS_REPLY;
      char buf[(widCliMapWorker.size()) * MAX_MEM_STATS_SIZE + 1];  //op + list + \0
      char* send_buf = client->GetFreeSlot();
      bool busy = false;
      if (send_buf == nullptr) {
        busy = true;
        send_buf = (char *) zmalloc(MAX_REQUEST_SIZE);
        epicLog(LOG_INFO,
                "We don't have enough slot buf, we use local buf instead");
      }

      int n = 0, i = 0;
      for (auto entry : widCliMapWorker) {
        //if(entry.first == client->GetWorkerId()) continue;
        Client* lc = entry.second;
        n += sprintf(buf + n, "%u:%d:%ld:%ld:", lc->GetQP(), lc->GetWorkerId(),
                     lc->GetTotalMem(), lc->GetFreeMem());
        i++;
      }
      lwr.size = i;  //widCliMapWorker.size()-1;
      epicAssert(widCliMapWorker.size() == i);
      lwr.ptr = buf;
      int len = 0, ret;
      lwr.Ser(send_buf, len);
      if ((ret = client->Send(send_buf, len)) != len) {
        epicAssert(ret == -1);
        epicLog(LOG_INFO, "slots are busy");
      }
      epicAssert((busy && ret == -1) || !busy);
      delete wr;
      wr = nullptr;
      break;
    }
    case PUT: {
      void* ptr = zmalloc(wr->size);
      memcpy(ptr, wr->ptr, wr->size);
      kvs[wr->key] = pair<void*, Size>(ptr, wr->size);
      if (to_serve_kv_request.count(wr->key)) {
        int size = to_serve_kv_request[wr->key].size();
        for (int i = 0; i < size; i++) {
          auto& to_serve = to_serve_kv_request[wr->key].front();
          epicAssert(to_serve.second->op == GET);
          epicLog(LOG_DEBUG, "processed to-serve remote kv request for key %ld",
                  to_serve.second->key);
          to_serve.second->flag |= TO_SERVE;
          to_serve_kv_request[wr->key].pop();
          ProcessRequest(to_serve.first, to_serve.second);
        }
        epicAssert(to_serve_kv_request[wr->key].size() == 0);
        to_serve_kv_request.erase(wr->key);
      }
      delete wr;
      wr = nullptr;
      break;
    }
    case GET: {
      if (kvs.count(wr->key)) {
        wr->ptr = kvs.at(wr->key).first;
        wr->size = kvs.at(wr->key).second;
        wr->op = GET_REPLY;
        char* send_buf = client->GetFreeSlot();
        bool busy = false;
        if (send_buf == nullptr) {
          busy = true;
          send_buf = (char *) zmalloc(MAX_REQUEST_SIZE);
          epicLog(LOG_INFO,
                  "We don't have enough slot buf, we use local buf instead");
        }

        int len = 0, ret;
        wr->Ser(send_buf, len);
        if ((ret = client->Send(send_buf, len)) != len) {
          epicAssert(ret == -1);
          epicLog(LOG_INFO, "slots are busy");
        }
        epicAssert((busy && ret == -1) || !busy);
        delete wr;
        wr = nullptr;
      } else {
        to_serve_kv_request[wr->key].push(
            pair<Client*, WorkRequest*>(client, wr));
      }

      break;
    }
    default:
      epicLog(LOG_WARNING, "unrecognized work request %d", wr->op);
      break;
  }
}

void Master::Broadcast(const char* buf, size_t len) {
  auto lt = qpCliMap.lock_table();
  for (auto entry : lt) {
    char* send_buf = entry.second->GetFreeSlot();
    bool busy = false;
    if (send_buf == nullptr) {
      busy = true;
      send_buf = (char *) zmalloc(MAX_REQUEST_SIZE);
      epicLog(LOG_INFO,
              "We don't have enough slot buf, we use local buf instead");
    }
    memcpy(send_buf, buf, len);

    size_t sent = entry.second->Send(send_buf, len);
    epicAssert((busy && sent == -1) || !busy);
    if (len != sent) {
      epicAssert(sent == -1);
      epicLog(LOG_INFO, "broadcast to %d failed (expected %d, but %d)\n", len,
              sent);
    }
  }
}
