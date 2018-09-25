// Copyright (c) 2018 The GAM Authors 

#include <cstring>
#include <arpa/inet.h>
#include "server.h"
#include "log.h"
#include "kernel.h"
#include "util.h"

Client* Server::NewClient() {
  Client* c = NewClient(IsMaster());
  return c;
}

Client* Server::NewClient(const char* rdmaConn) {
  return NewClient(IsMaster(), rdmaConn);
}

Client* Server::NewClient(bool isMaster, const char* rdmaConn) {
  try{
    Client* c = new Client(resource, isMaster, rdmaConn);
    uint32_t qp = c->GetQP();
    qpCliMap[qp] = c;
    return c;
  } catch (int err){
    epicLog(LOG_WARNING, "Unable to create new client\n");
    return NULL;
  }
}

void Server::ProcessRdmaRequest() {
  void *ctx;
  int ne;
  ibv_wc wc[MAX_CQ_EVENTS];
  ibv_cq *cq = resource->GetCompQueue();
  Client *cli;
  uint32_t immdata, id;
  int recv_c = 0;

  epicLog(LOG_DEBUG, "received RDMA event\n");
  /*
   * to get notified in the event-loop,
   * we need ibv_req_notify_cq -> ibv_get_cq_event -> ibv_ack_cq_events seq -> ibv_req_notify_cq!!
   */
  if (likely(resource->GetCompEvent())) {
    do {
      ne = ibv_poll_cq(cq, MAX_CQ_EVENTS, wc);
      if (unlikely(ne < 0)) {
        epicLog(LOG_FATAL, "Unable to poll cq\n");
        goto out;
      }

      for (int i = 0; i < ne; ++i) {
        /*
         * FIXME
         * 1) check whether the wc is initiated from the local host (ibv_post_send)
         * 2) if caused by ibv_post_send, then clear some stat used for selective signal
         *    otherwise, find the client, check the op code, process, and response if needed.
         */
        cli = FindClient(wc[i].qp_num);
        if (unlikely(!cli)) {
          epicLog(LOG_WARNING, "cannot find the corresponding client for qp %d\n", wc[i].qp_num);
          continue;
        }

        if(wc[i].status != IBV_WC_SUCCESS) {
          epicLog(LOG_WARNING, "Completion with error, op = %d (%d:%s)", wc[i].opcode, wc[i].status, ibv_wc_status_str(wc[i].status));
          continue;
        }

        epicLog(LOG_DEBUG, "transferred %d (qp_num %d, src_qp %d)", wc[i].byte_len, wc[i].qp_num, wc[i].src_qp);

        switch (wc[i].opcode) {
          case IBV_WC_SEND:
            epicLog(LOG_DEBUG, "get send completion event");
            id = cli->SendComp(wc[i]);
            FarmResumeTxn(cli);
            break;
          case IBV_WC_RECV:
            {

              epicLog(LOG_DEBUG, "Get recv completion event");
              char* data = cli->RecvComp(wc[i]);
              FarmProcessRemoteRequest(cli, data, wc[i].byte_len);
              recv_c++;
              break;
            }
          case IBV_WC_RDMA_WRITE:
          case IBV_WC_RECV_RDMA_WITH_IMM:
          default:
            epicLog(LOG_WARNING, "unknown opcode received %d\n", wc[i].opcode);
            break;
        }
      }
    } while (ne == MAX_CQ_EVENTS);

    if(recv_c) {
      //epicAssert(recv_c == resource->ClearRecv(low, high));
      int n = resource->PostRecv(recv_c);
      epicAssert(recv_c == n);
    }
  }

out:
  return;
}

Client* Server::FindClient(uint32_t qpn) {
  Client* cli = nullptr;
  try {
    cli = qpCliMap.at(qpn);
  } catch (const std::out_of_range& oor) {
    epicLog(LOG_WARNING, "cannot find the client for qpn %d (%s)", qpn, oor.what());
  }
  return cli;
}

void Server::UpdateWidMap() {
  int workers = IsMaster() ? qpCliMap.size() : qpCliMap.size()-1;
  if(widCliMap.size() < workers) {
    for(auto it = qpCliMap.begin(); it != qpCliMap.end(); it++) {
      int wid = it->second->GetWorkerId();
      if(wid == GetWorkerId()) {
        epicAssert(!IsMaster());
        continue; //ignore the client to the master
      }
      if(!widCliMap.count(wid)) {
        widCliMap[wid] = it->second;
      }
    }
    epicAssert(widCliMap.size() == workers);
  }
}

Client* Server::FindClientWid(int wid) {
  UpdateWidMap();

  Client* cli = nullptr;
  try {
    cli = widCliMap.at(wid);
  } catch (const std::out_of_range& oor) {
    epicLog(LOG_WARNING, "cannot find the client for worker %d (%s)", wid, oor.what());
  }

  return cli;
}

void Server::RmClient(Client* c) {
  qpCliMap.erase(c->GetQP());
}

