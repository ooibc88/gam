// Copyright (c) 2018 The GAM Authors 

#include <cstring>
#include <iostream>
#include <cstdlib>
#include <cassert>
#include <ctime>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "farm.h"
#include "workrequest.h"
#include "gallocator.h"
#include "log.h"

#define NOBJ 200000
#define TXOBJ 40
#define OSZIE 100
#define NWR 1

#define NRWR 1

int main() {
  ibv_device **list = ibv_get_device_list(NULL);
  int level = LOG_INFO;

  //master
  Conf* conf = new Conf();
  conf->loglevel = level;
  GAllocFactory::SetConf(conf);
  Master* master = new Master(*conf);

  char buf[OSZIE];
  for (int i = 0; i < OSZIE; i++) {
    buf[i] = 'a';
  }

  buf[OSZIE-1] = '\0';

  //worker1
  RdmaResource* res;

  Farm *f[NWR];

  GAddr a[NWR][NOBJ];

  for (int i = 0; i < NWR; i++) {
    conf = new Conf();
    conf->loglevel = level;
    res = new RdmaResource(list[0], false);
    conf->worker_port += i;
    f[i] = new Farm(new Worker(*conf, res));

    f[i]->txBegin();
    for (int j = 0; j < NOBJ; j++) {
      if (j % 10000 == 0)
        fprintf(stdout, "worker %d: %d-th object\n", i, j);
      a[i][j] = f[i]->txAlloc(OSZIE);
      f[i]->txWrite(a[i][j], buf, OSZIE);
    }
    f[i]->txCommit();
  }

  char c[OSZIE];
  GAddr b[NRWR][TXOBJ];
  int op[NRWR][TXOBJ];
  int r;
  int nr_commit = 0;
  int nr_abort = 0;

  clock_t t = clock();

  for (int k = 0; k < 10000; k++) {

    for (int j = 0; j < NRWR; j++) {
      f[j]->txBegin();
    }

    for (int j = 0; j < NRWR; j++) {
      for (int i = 0; i < TXOBJ; i++) {
        r = rand()%(NOBJ * NWR);
        b[j][i] = a[r/NOBJ][r%NOBJ];
        op[j][i] = rand() % 2;
        //op[j][i] = 0;
      }
    }

    for (int i = 0; i < TXOBJ; i++) {
      for (int j = 0; j < NRWR; j++) {
        if (op[j][i] == 0) {
          f[j]->txRead(b[j][i], c, OSZIE);
          assert(0 == strcmp(c, buf));
        } else {
          f[j]->txWrite(b[j][i], buf, OSZIE);
        }
      }
    }

    for (int j = 0; j < NRWR; j++) {
      if(f[j]->txCommit() != SUCCESS) {
        nr_abort++;
      } else
        nr_commit++;
    }
  }

  t = clock() - t;

  fprintf(stderr, "time:%f s, nr_commit = %d, nr_abort = %d, ratio = %f, thruput = %f\n", 
      ((float)t)/CLOCKS_PER_SEC,
      nr_commit,
      nr_abort,
      (float)nr_commit/(nr_commit + nr_abort),
      (nr_abort + nr_commit)*CLOCKS_PER_SEC/((float)t));

  //	master->Join();
  //	worker1->Join();
  //	worker2->Join();
  //	worker3->Join();
  sleep(2);
  epicLog(LOG_WARNING, "test done");

  return 0;
}


