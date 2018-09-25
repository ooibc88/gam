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

int main() {
  ibv_device **list = ibv_get_device_list(NULL);
  int level = LOG_INFO;

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
  int sz = 16;
  char buf[sz];
  char mbuf[sz];
  memcpy(buf, "hello, world!!!", sz);
  GAddr a1, a2, a3;
  memset(mbuf, 0, sz);

  f3->txBegin();
  a2 = f3->txAlloc(sz);
  assert(0 == f3->txPartialWrite(a2, 1, buf, sz));
  f3->txCommit(); 

  f1->txBegin();
  a2 = f1->txAlloc(sz);
  assert(sz == f1->txWrite(a2, buf, sz));
  f1->txCommit();

  f1->txBegin();
  assert(sz == f1->txRead(a2, mbuf, sz));
  assert(!strcmp("hello, world!!!", mbuf));
  memset(mbuf, 0, sz);
  assert(7 == f1->txPartialRead(a2, 7, mbuf, 7));
  fprintf(stdout, "read: %s, expect: world!!\n", mbuf);
  assert(!strcmp("world!!", mbuf));
  assert(7 == f1->txPartialWrite(a2, 0, mbuf, 7));
  assert(f1->txCommit() == SUCCESS);

  memset(mbuf, 0, sz);
  f1->txBegin();
  assert(sz == f1->txRead(a2, mbuf, sz));
  fprintf(stdout, "read: %s, expect: world!!world!!!\n", mbuf);
  assert(!strcmp("world!!world!!!", mbuf));
  assert(11 == f1->txPartialWrite(a2, 12, mbuf, 11));
  assert(f1->txCommit() != SUCCESS); // should fail

  memset(mbuf, 0, sz);
  f1->txBegin();
  assert(0 == f1->txPartialRead(a2, 11, mbuf, 7));
  fprintf(stdout, "read:%s, expect:\n", mbuf);
  assert(5 == f1->txPartialWrite(a2, 10, buf, 5));
  // now a2 becomes: "world!!worhello"
  assert(11 == f1->txPartialRead(a2, 5, mbuf, 11));
  fprintf(stdout, "read:%s, expect:%s\n", mbuf, "!!worhello");
  assert(!strcmp("!!worhello", mbuf));
  assert(f1->txCommit() == SUCCESS); // should fail again

  f1->txBegin();
  assert(sz == f1->txRead(a2, mbuf, sz));
  fprintf(stdout, "read:%s, expect:%s\n", mbuf, "world!!worhello");
  assert(!strcmp("world!!worhello", mbuf));
  assert(3 == f1->txPartialWrite(a2, 0, "   ", 3));
  assert(sz =f1->txRead(a2, mbuf, sz));
  fprintf(stdout, "read:%s, expect:%s\n", mbuf, "   ld!!worhello");
  assert(!strcmp(mbuf, "   ld!!worhello"));
  f1->txFree(a2);
  assert(f1->txCommit() == SUCCESS);

  f1->txBegin();
  assert(0 == f1->txRead(a2, mbuf, sz));
  assert(5 == f1->txWrite(a2, buf, 5));
  assert(SUCCESS != f1->txCommit());

  memset(mbuf, 0, sz);
  memcpy(buf, "hello, world!!!", sz);
  f1->put(0xffff, buf, sz);
  f1->get(0xffff, mbuf);
  fprintf(stdout, "%s\n", mbuf);
  memset(mbuf, 0, sz);
  f2->get(0xffff, mbuf);
  fprintf(stdout, "%s\n", mbuf);
  memset(mbuf, 0, sz);
  f3->get(0xffff, mbuf);
  fprintf(stdout, "%s\n", mbuf);
  //	master->Join();
  //	worker1->Join();
  //	worker2->Join();
  //	worker3->Join();
  sleep(2);
  epicLog(LOG_WARNING, "test done");

  return 0;
}
