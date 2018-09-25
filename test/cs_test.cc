// Copyright (c) 2018 The GAM Authors 


#include <cstring>
#include <iostream>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"
#include "util.h"

int main() {
  ibv_device **list = ibv_get_device_list(NULL);
  int level = LOG_WARNING;

  //master
  Conf* conf = new Conf();
  conf->loglevel = level;
  GAllocFactory::SetConf(conf);
  Master* master = new Master(*conf);

  //worker1
  conf = new Conf();
  conf->loglevel = level;
  RdmaResource* res = new RdmaResource(list[0], false);
  Worker *worker1, *worker2, *worker3;
  WorkerHandle *wh1, *wh2, *wh3;
  worker1 = new Worker(*conf, res);
  wh1 = new WorkerHandle(worker1);

  //worker2
  conf = new Conf();
  conf->loglevel = level;
  res = new RdmaResource(list[0], false);
  conf->worker_port += 1;
  worker2 = new Worker(*conf, res);
  wh2 = new WorkerHandle(worker2);

  //client1
  conf = new Conf();
  conf->loglevel = level;
  conf->cache_th = 1;
  res = new RdmaResource(list[0], false);
  conf->worker_port += 2;
  worker3 = new Worker(*conf, res);
  wh3 = new WorkerHandle(worker3);

  sleep(2);

  WorkRequest wr { };
  Size size = 10;
  GAddr gaddr;
  int i = 0;
  char buf[size];
  char wbuf[size];
  char local_init = 1;
  char remote_init = 2;

  for (i = 0; i < size; i++) {
    wr.Reset();
    wr.op = MALLOC;
    wr.size = size;
    wr.flag = RANDOM;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    gaddr = wr.addr;
    epicAssert(WID(wr.addr) != 3);
    epicLog(LOG_WARNING, "allocated %ld at %lx", size, gaddr);

    wr.Reset();
    wr.addr = gaddr;
    wr.op = FREE;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
  }

  for (i = 0; i < size; i++) {
    wr.Reset();
    wr.op = MALLOC;
    wr.size = size;
    wr.flag = REMOTE;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    gaddr = wr.addr;
    epicAssert(WID(wr.addr) != 3);
    epicLog(LOG_WARNING, "allocated %ld at %lx", size, gaddr);

    wr.Reset();
    wr.addr = gaddr;
    wr.op = FREE;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
  }

  for (i = 0; i < size; i++) {
    wr.Reset();
    wr.op = MALLOC;
    wr.size = size;
    wr.flag = RANDOM;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    gaddr = wr.addr;
    epicAssert(WID(wr.addr) != 3);
    epicLog(LOG_WARNING, "allocated %ld at %lx", size, gaddr);

    wr.Reset();
    wr.addr = gaddr;
    wr.op = FREE;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
  }

  for (i = 0; i < size; i++) {
    wr.Reset();
    wr.op = MALLOC;
    wr.size = size;
    wr.flag = REMOTE;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    gaddr = wr.addr;
    epicAssert(WID(wr.addr) != 3);
    epicLog(LOG_WARNING, "allocated %ld at %lx", size, gaddr);

    wr.Reset();
    wr.addr = gaddr;
    wr.op = FREE;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
  }

  sleep(2);
  epicLog(LOG_WARNING, "test done");

  return 0;
}

