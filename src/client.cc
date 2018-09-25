// Copyright (c) 2018 The GAM Authors 


#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <cerrno>
#include "rdma.h"
#include "anet.h"
#include "log.h"
#include "client.h"
#include "server.h"
#include "zmalloc.h"

Client::Client(RdmaResource* res, bool isForMaster, const char* rdmaConnStr)
    : resource(res) {
  wid = free = size = 0;
  this->ctx = res->NewRdmaContext(isForMaster);
  if (rdmaConnStr)
    this->SetRemoteConnParam(rdmaConnStr);
}

int Client::ExchConnParam(const char* ip, int port, Server* server) {
  //open the socket to exch rdma resouces
  char neterr[ANET_ERR_LEN];
  int sockfd = anetTcpConnect(neterr, const_cast<char *>(ip), port);
  if (sockfd < 0) {
    epicLog(LOG_WARNING, "Connecting to %s:%d %s", ip, port, neterr);
    exit(1);
  }

  const char* conn_str = GetConnString(server->GetWorkerId());
  int conn_len = strlen(conn_str);
  if (write(sockfd, conn_str, conn_len) != conn_len) {
    return -1;
  }

  char msg[conn_len + 1];
  /* waiting for server's response */
  int n = read(sockfd, msg, conn_len);
  if (n != conn_len) {
    epicLog(LOG_WARNING,
            "Failed to read conn param from server (%s; read %d bytes)\n",
            strerror(errno), n);
    return -1;
  }
  msg[n] = '\0';
  epicLog(LOG_INFO, "received conn string %s\n", msg);

  SetRemoteConnParam(msg);
  server->UpdateWidMap(this);

  /*
   * TODO: check whether this is needed!
   */
//    if (write(sockfd, ACK_CONN_STRING, sizeof ACK_CONN_STRING)) {
//        /*do  nothing */
//    }
  if (IsForMaster())
    server->PostConnectMaster(sockfd, server);

  close(sockfd);
  return 0;
}

int Client::SetRemoteConnParam(const char *conn) {
  const char* p = conn;
  if (resource->IsMaster()) {  //in the Master thread, connected to worker
    wid = resource->GetCounter();
  } else if (IsForMaster()) {  //in the worker thread, but connected to Master
    sscanf(conn, "%x:", &wid);
  } else if (!resource->IsMaster()) {  //in the worker thread, and connected to worker
    sscanf(conn, "%x:", &wid);
  } else {
    epicLog(LOG_WARNING, "undefined cases");
  }
  p = strchr(conn, ':');
  p++;
  return ctx->SetRemoteConnParam(p);
}

const char* Client::GetConnString(int workerid) {
  const char* rdmaConn = ctx->GetRdmaConnString();
  if (!connstr)
    connstr = (char*) zmalloc(MAX_CONN_STRLEN + 1);

  if (resource->IsMaster()) {  //in the Master thread
    sprintf(connstr, "%04x:%s", wid, rdmaConn);  //wid is already set
    epicLog(LOG_DEBUG, "master to worker here");
  } else if (IsForMaster()) {  //in the worker thread, but connected to Master
    sprintf(connstr, "%04x:%s", 0, rdmaConn);
    epicLog(LOG_DEBUG, "worker to master here");
  } else if (!resource->IsMaster()) {  //in the worker thread, and connected to worker
    epicAssert(workerid != 0);
    sprintf(connstr, "%04x:%s", workerid, rdmaConn);  //wid is the current worker id (not the remote pair's)
  } else {
    epicLog(LOG_WARNING, "undefined cases");
  }
  epicLog(LOG_DEBUG, "conn str %s\n", connstr);

  /* FIXME: rdmaConn does not get freed? rdmaConn is not null-terminated
   * string; will print it using %s format cause problems? */
  return connstr;
}

Client::~Client() {
  resource->DeleteRdmaContext(ctx);
}
