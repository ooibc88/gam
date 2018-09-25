// Copyright (c) 2018 The GAM Authors 

#include <cstring>
#include <iostream>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "farm.h"
#include "workrequest.h"
#include "gallocator.h"
#include "log.h"
#include <cassert>
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
  worker1 = new Worker(*conf, res);

  //worker2
  conf = new Conf();
  conf->loglevel = level;
  res = new RdmaResource(list[0], false);
  conf->worker_port += 1;
  worker2 = new Worker(*conf, res);

  int len;

  sleep(2);

  Farm* f1 =  new Farm(worker1);
  Farm* f2  = new Farm(worker1);
  Farm* f3 = new Farm(worker2);
  int sz = 1000;
  char buf[sz];
  for (int i = 0; i < sz; ++i)
  {
    buf[i] = 'a';
  }
  buf[sz-1] = 0;

  char mbuf[sz];

  GAddr a1, a2, a3;

  memset(mbuf, 0, sz);
  f1->txBegin();
  a3 = f1->txAlloc(sz);
  f1->txWrite(a3, buf, sz);
  f1->txRead(a3, mbuf, sz);
  assert(!strcmp(buf, mbuf));
  assert(f1->txCommit() == SUCCESS);

  memset(mbuf, 0, sz);
  f1->txBegin();
  f3->txBegin();
  a3 = f1->txAlloc(sz);
  assert(0 == f3->txRead(a3, mbuf, sz));
  a2 = f3->txAlloc(sz);
  assert(1 == f3->txWrite(a2, "hefs", 1));
  assert(f1->txCommit() == SUCCESS);
  assert(f3->txCommit() == SUCCESS);

  f3->txBegin();
  assert(0 == f3->txRead(a3, mbuf, sz));
  assert(f3->txCommit() == SUCCESS);

  f1->txBegin();
  f3->txBegin();
  assert(1 == f1->txRead(a2, mbuf, sz));
  //f3->txFree(a2);
  assert(sz == f1->txWrite(a2, mbuf, sz));
  assert(f1->txCommit() == SUCCESS);
  assert(f3->txCommit() == SUCCESS);

  f1->txBegin();
  f1->txWrite(a2, buf, sz);
  assert(f1->txCommit() == SUCCESS);

  f1->txBegin();
  f1->txRead(a2, mbuf, sz);
  fprintf(stdout, "%s\n", mbuf);
  assert(f1->txCommit() == SUCCESS);

  //    const char* value = "good";
  //    char vbuf[5];
  //    f1->kv_put(1, value, 4, 2);
  //    f1->kv_get(1, vbuf, 2);
  //    fprintf(stdout, "get key %d: %s\n", 1, vbuf);

  int vbuf;
  for(int i = 0; i < 10000; i++) {
    vbuf = 0;
    f1->kv_get(i, &vbuf, i%2+1);
    epicAssert(vbuf == 0);
    f1->kv_put(i, &i, sizeof(int), i%2+1);
    f1->kv_get(i, &vbuf, i%2+1);
    epicAssert(i == vbuf);

    vbuf = 0;
    f1->kv_put(i, &i, sizeof(int), i%2+1);
    f2->kv_get(i, &vbuf, i%2+1);
    epicAssert(i == vbuf);

    vbuf = 0;
    f2->kv_put(i, &i, sizeof(int), i%2+1);
    f2->kv_get(i, &vbuf, i%2+1);
    epicAssert(i == vbuf);
    //fprintf(stdout, "get key %d: %d\n", i, vbuf);
  }
  fprintf(stdout, "put/get succeed\n");

  long start = get_time();
  int it = 1000000;
  for(int i = 0; i < it; i++) {
    f1->kv_put(i, &i, sizeof(int), i%2+1);
    f1->kv_get(i, &vbuf, i%2+1);
    epicAssert(i == vbuf);
  }
  long end = get_time();
  fprintf(stdout, "put/get throughput = %lf, latency = %ld ns\n", (double)it/((double)(end-start)/1000/1000/1000)*2, (end-start)/it/2);


  //memset(mbuf, 0, sz);
  //memcpy(buf, "hello, world!!!", sz);
  //f1->put(0xffff, buf, sz);
  //sleep(1);
  //f2->get(0xffff, mbuf);
  //fprintf(stdout, "%s\n", mbuf);
end:
  //	master->Join();
  //	worker1->Join();
  //	worker2->Join();
  //	worker3->Join();
  sleep(2);
  epicLog(LOG_WARNING, "test done");

  return 0;
}
