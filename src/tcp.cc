// Copyright (c) 2018 The GAM Authors 

#include "tcp.h"
#include "rdma.h"
#include "log.h"
#include "server.h"
#include "client.h"
#include "anet.h"
#include "kernel.h"
#include "zmalloc.h"

#include <unistd.h>
#include <cerrno>
#include <cstring>

void AcceptTcpClientHandle(aeEventLoop *el, int fd, void *data, int mask) {
  epicAssert(data != nullptr);
  Server *server = (Server*) (data);
  char msg[MAX_CONN_STRLEN + 1];
  int n;
  const char *p;
  Client *cli;
  char neterr[ANET_ERR_LEN];
  char cip[IP_STR_LEN];
  int cfd, cport;

  cfd = anetTcpAccept(neterr, fd, cip, sizeof(cip), &cport);
  if (cfd == ANET_ERR) {
    if (errno != EWOULDBLOCK)
      epicLog(LOG_WARNING, "Accepting client connection: %s", neterr);
    return;
  }
  epicLog(LOG_INFO, "Accepted %s:%d", cip, cport);

  if (mask & AE_READABLE) {
    n = read(cfd, msg, sizeof msg);
    if (unlikely(n <= 0)) {
      epicLog(LOG_WARNING, "Unable to read conn string\n");
      goto out;
    }
    msg[n] = '\0';
    epicLog(LOG_INFO, "conn string %s\n", msg);
  }

  if (unlikely(!(cli = server->NewClient(msg)))) {
    goto out;
  }
  server->UpdateWidMap(cli);

  if (unlikely(!(p = cli->GetConnString(server->GetWorkerId())))) {
    goto out;
  }

  server->UpdateWidMap(cli);

  n = write(cfd, p, strlen(p));

  if (unlikely(n < strlen(p))) {
    epicLog(LOG_WARNING, "Unable to send conn string\n");
    server->RmClient(cli);
  }

  if (server->IsMaster())
    server->PostAcceptWorker(cfd, server);

  out: close(cfd);
}

void ProcessRdmaRequestHandle(aeEventLoop *el, int fd, void *data, int mask) {
  ((Server *) data)->ProcessRdmaRequest();
}
