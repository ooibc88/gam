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

  //worker3
  conf = new Conf();
  conf->loglevel = level;
  res = new RdmaResource(list[0], false);
  conf->worker_port += 2;
  worker3 = new Worker(*conf, res);
  wh3 = new WorkerHandle(worker3);

  sleep(2);

  WorkRequest wr { };
  Size size = 8;
  GAddr gaddr;
  int i = 0;
  char buf[size];
  char wbuf[size];
  char local_init = 0;
  char remote_init = 0;

  //init the data to be written
  for (i = 0; i < size; i++) {
    wbuf[i] = (char) i;
  }

  //local allocate
  wr.Reset();
  wr.op = MALLOC;
  wr.size = size;
  if (wh1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  gaddr = wr.addr;
  epicLog(LOG_WARNING, "allocated %ld at %lx", size, gaddr);

  //local read
  wr.Reset();
  wr.op = READ;
  wr.addr = gaddr;
  wr.size = size;
  wr.ptr = buf;
  if (wh1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != local_init) {
      epicLog(LOG_WARNING, "local read failed at buf[%d] (%d) != %d\n", i,
              buf[i], local_init);
      break;
    }
  }
  if (i == size) {
    epicLog(LOG_WARNING, "local read succeed!");
  } else {
    epicLog(LOG_WARNING, "local read failed!");
    exit(1);
  }

  //local write
  memset(buf, 0, size);
  wr.Reset();
  wr.op = WRITE;
  wr.addr = gaddr;
  wr.size = size;
  wr.ptr = wbuf;
  wr.flag = ASYNC;
  if (wh1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  wr.Reset();
  wr.addr = gaddr;
  wr.size = size;
  wr.op = READ;
  wr.ptr = buf;
  if (wh1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != wbuf[i]) {
      epicLog(LOG_WARNING, "local read failed at buf[%d] (%d) != %d\n", i,
              buf[i], wbuf[i]);
      break;
    }
  }
  if (i == size) {
    epicLog(LOG_WARNING, "local write succeed!");
  } else {
    epicLog(LOG_WARNING, "local write failed!");
    exit(1);
  }

  //remote read from worker2 (READ Case 2, cflag = null)
  memset(buf, 0, size);
  wr.Reset();
  wr.addr = gaddr;
  wr.size = size;
  wr.op = READ;
  wr.ptr = buf;
  if (wh2->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != wbuf[i]) {
      epicLog(LOG_WARNING,
              "READ Case 2 (null): remote read failed at buf[%d] (%d) != %d\n",
              i, buf[i], wbuf[i]);
      break;
    }
  }
  if (i == size) {
    epicLog(LOG_WARNING, "READ Case 2 (null): succeed!");
  } else {
    epicLog(LOG_WARNING, "READ Case 2 (null): failed!");
    exit(1);
  }

  //remote read from worker3 (READ Case 2, cflag = shared)
  memset(buf, 0, size);
  wr.Reset();
  wr.addr = gaddr;
  wr.size = size;
  wr.op = READ;
  wr.ptr = buf;
  if (wh3->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != wbuf[i]) {
      epicLog(
          LOG_WARNING,
          "READ Case 2 (shared): remote read failed at buf[%d] (%d) != %d\n", i,
          buf[i], wbuf[i]);
      break;
    }
  }
  if (i == size) {
    epicLog(LOG_WARNING, "READ Case 2 (shared): succeed!");
  } else {
    epicLog(LOG_WARNING, "READ Case 2 (shared): failed!");
    exit(1);
  }

  //remote write from worker3 (WRITE Case 3, cflag = shared, cached)
  memset(buf, 0, size);
  for (i = 0; i < size; i++) {
    wbuf[i] = (char) (i + 1);
  }
  wr.Reset();
  wr.addr = gaddr;
  wr.size = size;
  wr.op = WRITE;
  wr.ptr = wbuf;
  wr.flag = ASYNC;
  if (wh3->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  wr.Reset();
  wr.addr = gaddr;
  wr.size = size;
  wr.op = READ;
  wr.ptr = buf;
  if (wh3->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != wbuf[i]) {
      epicLog(
          LOG_WARNING,
          "WRITE Case 3 (shared, cached): remote read failed at buf[%d] (%d) != %d\n",
          i, buf[i], wbuf[i]);
      break;
    }
  }
  if (i == size) {
    epicLog(LOG_WARNING, "WRITE Case 3 (shared, cached): succeed!");
  } else {
    epicLog(LOG_WARNING, "WRITE Case 3 (shared, cached): failed!");
    exit(1);
  }

  //read from worker1 (READ Case 1, dflag = dirty)
  memset(buf, 0, size);
  wr.Reset();
  wr.addr = gaddr;
  wr.size = size;
  wr.op = READ;
  wr.ptr = buf;
  if (wh1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != wbuf[i]) {
      epicLog(LOG_WARNING,
              "READ Case 1: remote read failed at buf[%d] (%d) != %d\n", i,
              buf[i], wbuf[i]);
      break;
    }
  }
  if (i == size) {
    epicLog(LOG_WARNING, "READ Case 1: succeed!");
  } else {
    epicLog(LOG_WARNING, "READ Case 1: failed!");
    exit(1);
  }

  {   // (WRITE Case 1, dflag = shared)
    //read from worker2
    memset(buf, 0, size);
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh2->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (buf[i] != wbuf[i]) {
        epicLog(
            LOG_WARNING,
            "WRITE Case 1 - step 1: remote read failed at buf[%d] (%d) != %d\n",
            i, buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING, "WRITE Case 1　- step 1: succeed!");
    } else {
      epicLog(LOG_WARNING, "WRITE Case 1 - step 1: failed!");
      exit(1);
    }

    //and write from worker1
    memset(buf, 0, size);
    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (wbuf[i] + 1);
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = WRITE;
    wr.ptr = wbuf;
    wr.flag = ASYNC;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (buf[i] != wbuf[i]) {
        epicLog(LOG_WARNING,
                "READ Case 1: remote read failed at buf[%d] (%d) != %d\n", i,
                buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING, "WRITE Case 1 - step 2: succeed!");
    } else {
      epicLog(LOG_WARNING, "WRITE Case 1 - step 2: failed!");
      exit(1);
    }
  }

  sleep(1);
  {
    //WRITE Case 2
    //write from worker2 (WRITE Case 3, null)
    memset(buf, 0, size);
    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (wbuf[i] + 1);
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = WRITE;
    wr.ptr = wbuf;
    wr.flag = ASYNC;
    if (wh2->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh2->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (buf[i] != wbuf[i]) {
        epicLog(
            LOG_WARNING,
            "WRITE Case 2 - step 1: remote read failed at buf[%d] (%d) != %d\n",
            i, buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING, "WRITE Case 2　- step 1: succeed!");
    } else {
      epicLog(LOG_WARNING, "WRITE Case 2 - step 1: failed!");
      exit(1);
    }

    //and write from worker1
    memset(buf, 0, size);
    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (wbuf[i] + 1);
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = WRITE;
    wr.ptr = wbuf;
    wr.flag = ASYNC;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (buf[i] != wbuf[i]) {
        epicLog(LOG_WARNING,
                "WRITE Case 2: remote read failed at buf[%d] (%d) != %d\n", i,
                buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING, "WRITE Case 2 - step 2: succeed!");
    } else {
      epicLog(LOG_WARNING, "WRITE Case 2 - step 2: failed!");
      exit(1);
    }
  }

  sleep(1);
  {
    //WRITE Case 3 (shared, non-cached)
    //read from worker2
    memset(buf, 0, size);
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh2->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (buf[i] != wbuf[i]) {
        epicLog(
            LOG_WARNING,
            "WRITE Case 3 (shared, non-cached) - step 1: remote read failed at buf[%d] (%d) != %d\n",
            i, buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING,
              "WRITE Case 3 (shared, non-cached)　- step 1: succeed!");
    } else {
      epicLog(LOG_WARNING,
              "WRITE Case 3 (shared, non-cached) - step 1: failed!");
      exit(1);
    }

    //and write from worker3
    memset(buf, 0, size);
    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (wbuf[i] + 1);
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.op = WRITE;
    wr.ptr = wbuf;
    wr.size = size - 1;
    wr.flag = ASYNC;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (i == size - 1 && buf[i] == (char) (wbuf[i] - 1))
        continue;
      if (buf[i] != wbuf[i]) {
        epicLog(
            LOG_WARNING,
            "WRITE Case 3 (shared, non-cached): remote read failed at buf[%d] (%d) != %d\n",
            i, buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING,
              "WRITE Case 3 (shared, non-cached) - step 2: succeed!");
    } else {
      epicLog(LOG_WARNING,
              "WRITE Case 3 (shared, non-cached) - step 2: failed!");
      exit(1);
    }
  }

  sleep(1);
  {
    //Read Case 3
    //read from worker2
    memset(buf, 0, size);
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh2->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (i == size - 1 && buf[i] == (char) (wbuf[i] - 1))
        continue;
      if (buf[i] != wbuf[i]) {
        epicLog(LOG_WARNING,
                "Read Case 3: remote read failed at buf[%d] (%d) != %d\n", i,
                buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING, "***************Read Case 3: succeed!************");
    } else {
      epicLog(LOG_WARNING, "Read Case 3: failed!");
      exit(1);
    }
  }

  sleep(1);
  {
    //Write Case 3 (shared, cached)
    //write from worker2
    memset(buf, 0, size);
    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (wbuf[i] + 1);
    }
    wr.Reset();
    wr.addr = GADD(gaddr, 1);
    wr.op = WRITE;
    wr.ptr = wbuf + 1;
    wr.size = size - 1;
    wr.flag = ASYNC;
    if (wh2->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.op = READ;
    wr.ptr = buf;
    wr.size = size;
    if (wh2->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (i == 0 && buf[i] == (char) (wbuf[i] - 1))
        continue;
      if (buf[i] != wbuf[i]) {
        epicLog(
            LOG_WARNING,
            "WRITE Case 3 (shared, cached): remote read failed at buf[%d] (%d) != %d\n",
            i, buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(
          LOG_WARNING,
          "**************Write Case 3 (shared, cached): succeed!******************");
    } else {
      epicLog(LOG_WARNING, "Write Case 3 (shared, cached): failed!");
      exit(1);
    }
  }

  sleep(1);
  {
    //Write Case 4
    //write from worker3
    memset(buf, 0, size);
    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (wbuf[i] + 1);
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = WRITE;
    wr.ptr = wbuf;
    wr.flag = ASYNC;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    wr.Reset();
    wr.addr = gaddr;
    wr.size = size;
    wr.op = READ;
    wr.ptr = buf;
    if (wh3->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (buf[i] != wbuf[i]) {
        epicLog(LOG_WARNING,
                "WRITE Case 4: remote read failed at buf[%d] (%d) != %d\n", i,
                buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING,
              "**************Write Case 4: succeed!******************");
    } else {
      epicLog(LOG_WARNING, "Write Case 4: failed!");
      exit(1);
    }
  }

  end:
//	master->Join();
//	worker1->Join();
//	worker2->Join();
//	worker3->Join();
  sleep(2);
  epicLog(LOG_WARNING, "test done");

  return 0;
}

