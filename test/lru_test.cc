// Copyright (c) 2018 The GAM Authors 


/*
 * fence_test.cc
 *
 *  Created on: Apr 4, 2016
 *      Author: zhanghao
 */
#include <cstring>
#include <iostream>
#include <thread>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"

#define USE_LOCK
#define USE_TRY_LOCK

WorkerHandle *wh1_1, *wh1_2, *wh2_1, *wh2_2, *wh3_1, *wh3_2;
int block = 10;
Size size = BLOCK_SIZE;
GAddr g1, g2, g3;
char local_init = 1;
char remote_init = 2;
int MAX_CASES = 100;
int ng1 = 100000, ng2 = 20000, ng3 = 100000, ng4 = 50000;

//using wh1_1
void thread1_1() {
  epicLog(LOG_WARNING, "*****************start thread1**********************");
  char buf[size], wbuf[size];
  int i;
  WorkRequest* wr = new WorkRequest();
  //init the data to be written

  for (int j = 0; j < block; j++) {
#ifdef USE_LOCK
    //rlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = WLOCK;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    int i = 0;
    while (wh1_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = GADD(g1, j * size);
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      //sleep(1);
    }
    epicLog(LOG_WARNING, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = WRITE;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh1_1->SendRequest(wr)) {
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
      epicLog(LOG_WARNING,
              "*****local write (thread1_1 %d) succeed buf[0] = %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "local write (thread1_1 %d) failed!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = UNLOCK;
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
  }

  epicLog(LOG_WARNING, "end thread1_1");
  delete wr;
  sleep(2);
}

//using wh1_2
void thread1_2() {
  epicLog(LOG_WARNING, "*****************start thread2**********************");
  char buf[size], wbuf[size];
  WorkRequest* wr = new WorkRequest();
  int i;

  for (int j = 0; j < block; j++) {
#ifdef USE_LOCK
    //rlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = RLOCK;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    int i = 0;
    while (wh1_2->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = GADD(g1, j * size);
      wr->op = RLOCK;
      wr->flag |= TRY_LOCK;
      //sleep(1);
    }
    epicLog(LOG_WARNING, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif

    //rlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    ;
    wr->op = UNLOCK;
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //wlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = WLOCK;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh1_2->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = GADD(g1, j * size);
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      //sleep(1);
    }
    epicLog(LOG_WARNING, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = WRITE;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = wbuf;
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh1_2->SendRequest(wr)) {
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
      epicLog(LOG_WARNING,
              "*****local write (thread1_2 %d) succeed, buf[0] = %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "local write (thread1_2 %d) failed!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //rlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = UNLOCK;
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
  }

  epicLog(LOG_WARNING, "end thread1_2");

  delete wr;
  sleep(2);
}

//using wh2_1
void thread2_1() {
  epicLog(LOG_WARNING, "*****************start thread3**********************");
  char buf[size], wbuf[size];
  WorkRequest* wr = new WorkRequest();
  int i;

  for (int j = 0; j < block; j++) {
#ifdef USE_LOCK
    //rlock remote address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = WLOCK;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh2_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = GADD(g1, j * size);
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      //sleep(1);
    }
    epicLog(LOG_WARNING, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = WRITE;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    for (i = 0; i < size; i++) {
      if (buf[i] != wbuf[i]) {
        epicLog(LOG_WARNING, "remote read failed at buf[%d] (%d) != %d\n", i,
                buf[i], wbuf[i]);
        break;
      }
    }
    if (i == size) {
      epicLog(LOG_WARNING,
              "*****remote write succeed (thread2_1 %d), buf[0] =  %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "remote write failed (thread2_1 %d)!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock remote address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = UNLOCK;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
  }

  epicLog(LOG_WARNING, "end thread2_1");

  delete wr;
  sleep(2);
}

//using wh3_1
void thread3_1() {
  epicLog(LOG_WARNING, "*****************start thread4**********************");
  char buf[size], wbuf[size];
  int i;
  WorkRequest* wr = new WorkRequest();
  //init the data to be written

  for (int j = 0; j < block; j++) {
#ifdef USE_LOCK
    //rlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = WLOCK;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh3_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = GADD(g1, j * size);
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      //sleep(1);
    }
    epicLog(LOG_WARNING, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = WRITE;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    memset(wr, 0, sizeof(WorkRequest));
    wr->op = READ;
    wr->addr = GADD(g1, j * size);
    wr->size = size;
    wr->ptr = buf;
    if (wh3_1->SendRequest(wr)) {
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
      epicLog(LOG_WARNING,
              "*****local write (thread3_1 %d) succeed buf[0] = %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "local write (thread3_1 %d) failed!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock local address
    memset(wr, 0, sizeof(WorkRequest));
    wr->addr = GADD(g1, j * size);
    wr->op = UNLOCK;
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
  }

  delete wr;
  sleep(2);
}

int main() {
  ibv_device **list = ibv_get_device_list(NULL);
  int i;

  //master
  Conf* conf = new Conf();
  //conf->loglevel = LOG_WARNING;
  GAllocFactory::SetConf(conf);
  Master* master = new Master(*conf);

  //worker1
  conf = new Conf();
  RdmaResource* res = new RdmaResource(list[0], false);
  Worker *worker1, *worker2, *worker3;
  worker1 = new Worker(*conf, res);
  wh1_1 = new WorkerHandle(worker1);
  wh1_2 = new WorkerHandle(worker1);

  //worker2
  conf = new Conf();
  res = new RdmaResource(list[0], false);
  conf->worker_port += 1;
  worker2 = new Worker(*conf, res);
  wh2_1 = new WorkerHandle(worker2);
  wh2_2 = new WorkerHandle(worker2);

  //worker3
  conf = new Conf();
  res = new RdmaResource(list[0], false);
  conf->worker_port += 2;
  worker3 = new Worker(*conf, res);
  wh3_1 = new WorkerHandle(worker3);
  wh3_2 = new WorkerHandle(worker3);

  sleep(1);

  char buf[size];
  char wbuf[size];
  WorkRequest wr { };

  //init the data to be written
  for (i = 0; i < size; i++) {
    wbuf[i] = (char) i;
  }

  //w1 allocate
  wr.op = MALLOC;
  wr.size = size * block;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  g1 = wr.addr;
  epicLog(LOG_WARNING, "\n****allocated g1 %ld at %lx*****\n", size, g1);

  //init the data to be written
  for (i = 0; i < size; i++) {
    wbuf[i] = (char) i;
  }
  wr.op = WRITE;
  wr.addr = g1;
  wr.size = size;
  wr.ptr = wbuf;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  wr.op = READ;
  wr.ptr = buf;
  if (wh1_1->SendRequest(&wr)) {
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

  sleep(1);

  thread* t1_1 = new thread(thread1_1);
  thread* t1_2 = new thread(thread1_2);
  thread* t2_1 = new thread(thread2_1);
  thread* t3_1 = new thread(thread3_1);

  t1_1->join();
  t1_2->join();
  t2_1->join();
  t3_1->join();

#ifdef USE_LOCK
  wr.addr = g1;
  wr.op = WLOCK;
  wr.flag = 0;
  wr.size = 0;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
#endif

  wr.op = READ;
  wr.ptr = buf;
  wr.size = size;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != (char) (wbuf[i] + 4)) {
      epicLog(LOG_WARNING, "local read failed at buf[%d] (%d) != %d\n", i,
              buf[i], (char )(wbuf[i] + 4));
      break;
    }
  }
  if (i == size) {
    epicLog(LOG_WARNING, "multi-threaded add succeed!");
  } else {
    epicLog(LOG_WARNING, "multi-threaded add failed!");
    exit(1);
  }

#ifdef USE_LOCK
  wr.addr = g1;
  wr.op = UNLOCK;
  wr.size = 0;
  wr.flag = 0;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
#endif

  sleep(10);
  epicLog(LOG_WARNING, "test done");
  return 0;
}

