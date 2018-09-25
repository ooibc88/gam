// Copyright (c) 2018 The GAM Authors 

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
Size size = BLOCK_SIZE;  //eight blocks
GAddr g1, g2, g3;
char local_init = 1;
char remote_init = 2;
int MAX_CASES = 100;
int ng1 = 1000, ng2 = 2000, ng3 = 1000, ng4 = 1000;

//using wh1_1
void thread1_1() {
  epicLog(LOG_WARNING,
          "*****************start thread1_1**********************");
  char buf[size], wbuf[size];
  int i;
  WorkRequest* wr = new WorkRequest();
  //init the data to be written

  for (int j = 0; j < ng1; j++) {
#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = RLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    int i = 0;
    while (wh1_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = RLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
    //unlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif

#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = WLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh1_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    wr->Reset();
    wr->op = WRITE;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh1_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
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
      epicLog(LOG_INFO,
              "*****local write (thread1_1 %d) succeed buf[0] = %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "local write (thread1_1 %d) failed!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    wr->size = 0;
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
  epicLog(LOG_WARNING,
          "*****************start thread1_2**********************");
  char buf[size], wbuf[size];
  WorkRequest* wr = new WorkRequest();
  int i;

  for (int j = 0; j < ng2; j++) {
#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = RLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    int i = 0;
    while (wh1_2->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = RLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif

    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#ifdef USE_LOCK
    //wlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = WLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh1_2->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    wr->Reset();
    wr->op = WRITE;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh1_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
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
      epicLog(LOG_INFO,
              "*****local write (thread1_2 %d) succeed, buf[0] = %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "local write (thread1_2 %d) failed!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    wr->size = 0;
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
  epicLog(LOG_WARNING,
          "*****************start thread2_1**********************");
  char buf[size], wbuf[size];
  WorkRequest* wr = new WorkRequest();
  int i;

  for (int j = 0; j < ng3; j++) {
#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = RLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    int i = 0;
    while (wh2_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = RLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
    //unlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif

#ifdef USE_LOCK
    //rlock remote address
    wr->Reset();
    wr->addr = g1;
    wr->op = WLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh2_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    wr->Reset();
    wr->op = WRITE;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
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
      epicLog(LOG_INFO,
              "*****remote write succeed (thread2_1 %d), buf[0] =  %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "remote write failed (thread2_1 %d)!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock remote address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    wr->size = 0;
    if (wh2_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
  }

  epicLog(LOG_WARNING, "end thread2_1");

  delete wr;
  sleep(2);
}

//using wh2_2
void thread2_2() {
  epicLog(LOG_WARNING,
          "*****************start thread2_2**********************");
  char buf[size], wbuf[size];
  WorkRequest* wr = new WorkRequest();
  int i;

  for (int j = 0; j < ng3; j++) {
#ifdef USE_LOCK
    //rlock remote address
    wr->Reset();
    wr->addr = g1;
    wr->op = WLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh2_2->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh2_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh2_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    wr->Reset();
    wr->op = WRITE;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh2_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh2_2->SendRequest(wr)) {
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
      epicLog(LOG_INFO,
              "*****remote write succeed (thread2_1 %d), buf[0] =  %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "remote write failed (thread2_1 %d)!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock remote address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    wr->size = 0;
    if (wh2_2->SendRequest(wr)) {
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
  epicLog(LOG_WARNING,
          "*****************start thread3_1**********************");
  char buf[size], wbuf[size];
  int i;
  WorkRequest* wr = new WorkRequest();
  //init the data to be written

  for (int j = 0; j < ng4; j++) {
#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = RLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    int i = 0;
    while (wh3_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = RLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
    //unlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif

#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = WLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh3_1->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      wr->size = 0;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    wr->Reset();
    wr->op = WRITE;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
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
      epicLog(LOG_INFO,
              "*****local write (thread1_1 %d) succeed buf[0] = %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "local write (thread1_1 %d) failed!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    wr->size = 0;
    if (wh3_1->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
  }
  epicLog(LOG_WARNING, "end thread3_1");
  delete wr;
  sleep(2);
}

//using wh3_2
void thread3_2() {
  epicLog(LOG_WARNING,
          "*****************start thread3_2**********************");
  char buf[size], wbuf[size];
  int i;
  WorkRequest* wr = new WorkRequest();
  //init the data to be written

  for (int j = 0; j < ng4; j++) {
#ifdef USE_LOCK
    //rlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = WLOCK;
    wr->size = 0;
#ifdef USE_TRY_LOCK
    wr->flag |= TRY_LOCK;
    i = 0;
    while (wh3_2->SendRequest(wr)) {
      //epicLog(LOG_WARNING, "try to lock %lx (%d time)", wr->addr, i);
      i++;
      wr->Reset();
      wr->addr = g1;
      wr->op = WLOCK;
      wr->flag |= TRY_LOCK;
      //sleep(1);
    }
    epicLog(LOG_INFO, "@@@@@@lock succeed (%d time)@@@@@@", i);
#else
    if (wh3_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
#endif

    //local read
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh3_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    for (i = 0; i < size; i++) {
      wbuf[i] = (char) (buf[i] + 1);
    }

    //local write
    wr->Reset();
    wr->op = WRITE;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = wbuf;
    wr->flag = ASYNC;
    if (wh3_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    //local read again
    wr->Reset();
    wr->op = READ;
    wr->addr = g1;
    wr->size = size;
    wr->ptr = buf;
    if (wh3_2->SendRequest(wr)) {
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
      epicLog(LOG_INFO,
              "*****local write (thread1_1 %d) succeed buf[0] = %d!*****", j,
              buf[0]);
    } else {
      epicLog(LOG_WARNING, "local write (thread1_1 %d) failed!", j);
      exit(1);
    }

#ifdef USE_LOCK
    //unlock local address
    wr->Reset();
    wr->addr = g1;
    wr->op = UNLOCK;
    wr->flag = ASYNC;
    wr->size = 0;
    if (wh3_2->SendRequest(wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
#endif
  }
  epicLog(LOG_WARNING, "end thread3_2");
  delete wr;
  sleep(2);
}

int main() {
  ibv_device **list = ibv_get_device_list(NULL);
  int i;

  //master
  Conf* conf = new Conf();
  conf->loglevel = LOG_WARNING;
  //conf->loglevel = LOG_DEBUG;
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
  wr.Reset();
  wr.op = MALLOC;
  wr.size = size;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  g1 = wr.addr;
  epicLog(LOG_WARNING, "\n****allocated g1 %ld at %lx*****\n", size, g1);

  //fine-grained lock works for threads within a physical node
  wr.Reset();
  wr.addr = g1;
  wr.op = WLOCK;
  if (wh2_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "remotely lock g1 succeed\n");

  wr.Reset();
  wr.addr = g1 + 1;
  wr.op = WLOCK;
  if (wh2_2->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "remotely lock g1+1 succeed\n");

  wr.Reset();
  wr.addr = g1;
  wr.op = UNLOCK;
  wr.flag = ASYNC;
  if (wh2_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "remotely unlock g1 succeed\n");

  wr.Reset();
  wr.addr = g1 + 1;
  wr.op = UNLOCK;
  wr.flag = ASYNC;
  if (wh2_2->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "remotely unlock g1+1 succeed\n");

  wr.Reset();
  wr.addr = g1;
  wr.op = WLOCK;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "locally lock g1 succeed\n");

  wr.Reset();
  wr.addr = g1 + 1;
  wr.op = WLOCK;
  if (wh1_2->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "locally lock g1+1 succeed\n");

  wr.Reset();
  wr.addr = g1;
  wr.op = UNLOCK;
  wr.flag = ASYNC;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "locally unlock g1 succeed\n");

  wr.Reset();
  wr.addr = g1 + 1;
  wr.op = UNLOCK;
  wr.flag = ASYNC;
  if (wh1_2->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  epicLog(LOG_WARNING, "locally unlock g1+1 succeed\n");

//	//w2 allocate
//	wr.addr = 0;
//	if(wh2_1->SendRequest(&wr)) {
//		epicLog(LOG_WARNING, "send request failed");
//	}
//	g2 = wr.addr;
//	epicLog(LOG_WARNING, "\n****allocated g2 %ld at %lx*****\n", size, g2);
//
//
//	//w3 allocate
//	wr.addr = 0;
//	if(wh3_1->SendRequest(&wr)) {
//		epicLog(LOG_WARNING, "send request failed");
//	}
//	g3 = wr.addr;
//	epicLog(LOG_WARNING, "\n****allocated g3 %ld at %lx*****\n", size, g3);

  //init the data to be written
  for (i = 0; i < size; i++) {
    wbuf[i] = (char) i;
  }
  wr.Reset();
  wr.op = WRITE;
  wr.addr = g1;
  wr.size = size;
  wr.ptr = wbuf;
  wr.flag = ASYNC;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  wr.Reset();
  wr.addr = g1;
  wr.size = size;
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
  thread* t2_2 = new thread(thread2_2);
  thread* t3_1 = new thread(thread3_1);
  thread* t3_2 = new thread(thread3_2);

  t1_1->join();
  t1_2->join();
  t2_1->join();
  t2_2->join();
  t3_1->join();
  t3_2->join();

#ifdef USE_LOCK
  wr.Reset();
  wr.addr = g1;
  wr.op = WLOCK;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
#endif

  wr.Reset();
  wr.addr = g1;
  wr.op = READ;
  wr.ptr = buf;
  wr.size = size;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  for (i = 0; i < size; i++) {
    if (buf[i] != (char) (wbuf[i] + ng1 + ng2 + ng3 * 2 + ng4 * 2)) {
      //if(buf[i] != (char)(wbuf[i]+ng1+ng3)) {
      epicLog(LOG_WARNING, "local read failed at buf[%d] (%d) != %d\n", i,
              buf[i], (char )(wbuf[i] + ng1 + ng2 + ng3 * 2 + ng4 * 2));
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
  wr.Reset();
  wr.addr = g1;
  wr.op = UNLOCK;
  wr.flag = ASYNC;
  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
#endif

  sleep(2);
  epicLog(LOG_WARNING, "test done");
  return 0;
}

