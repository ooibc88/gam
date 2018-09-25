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

WorkerHandle *wh1, *wh2, *wh3;
Size size = sizeof(int);
GAddr g1, g2, g3;
char local_init = 1;
char remote_init = 2;
int MAX_CASES = 100;
int ng1 = 1, ng2 = 1, ng3 = 1;
int iteration = 1 << 10;

//#define NOFENCE

//using wh1
void thread1() {
  epicLog(LOG_WARNING, "*****************start thread1**********************");
  int buf, wbuf = 0x01010101;
  WorkRequest* wr = new WorkRequest();

  for (int i = 0; i < iteration; i++) {
    {
      ng2++;
      ng1++;
      ng3++;
      //0. remote write
      wbuf = ng2;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g2;
      wr->size = size;
      wr->flag = ASYNC;
      wr->op = WRITE;
      wr->ptr = &wbuf;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }

    {
      //remote read g2
      buf = 0;
      wbuf = ng2;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g2;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &buf;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
      if (buf == wbuf) {
        epicLog(LOG_INFO, "remote read g2 succeed");
      } else {
        epicLog(LOG_WARNING, "remote read g2 failed expect %d, but %d", wbuf,
                buf);
        exit(1);
      }
    }

#ifndef NOFENCE
    {
      //mfence
      memset(wr, 0, sizeof(WorkRequest));
      wr->flag = ASYNC;
      wr->op = MFENCE;
      wr->flag = ASYNC;
      wr->ptr = nullptr;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }
#endif

    {
      //1. local write
      wbuf = ng1;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g1;
      wr->size = size;
      wr->flag = ASYNC;
      wr->op = WRITE;
      wr->ptr = &wbuf;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }

#ifndef NOFENCE
    {
      //mfence
      memset(wr, 0, sizeof(WorkRequest));
      wr->flag = ASYNC;
      wr->op = MFENCE;
      wr->flag = ASYNC;
      wr->ptr = nullptr;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }
#endif

    {
      //remote write g3
      buf = 0;
      wbuf = ng3;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g3;
      wr->size = size;
      wr->flag = ASYNC;
      wr->op = WRITE;
      wr->ptr = &wbuf;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }

#ifndef NOFENCE
    {
      //mfence
      memset(wr, 0, sizeof(WorkRequest));
      wr->flag = ASYNC;
      wr->op = MFENCE;
      wr->flag = ASYNC;
      wr->ptr = nullptr;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }
#endif

    {
      //remote read g3
      buf = 0;
      wbuf = ng3;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g3;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &buf;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
      if (buf == wbuf) {
        epicLog(LOG_INFO, "remote read g3 succeed");
      } else {
        epicLog(LOG_WARNING, "remote read g3 failed expect %d, but %d", wbuf,
                buf);
        exit(1);
      }
    }

    {
      //2. local read
      buf = 0;
      wbuf = ng1;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g1;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &buf;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
      if (buf == wbuf) {
        epicLog(LOG_INFO, "local read succeed");
      } else {
        epicLog(LOG_WARNING, "local read failed expect %d, but %d", wbuf, buf);
        exit(1);
      }
    }

    {
      //3. remote read
      buf = 0;
      wbuf = ng2;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g2;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &buf;
      if (wh1->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
      if (buf == wbuf) {
        epicLog(LOG_INFO, "remote read succeed");
      } else {
        epicLog(LOG_WARNING, "remote read failed expect %d, but %d", wbuf, buf);
        exit(1);
      }
    }
  }

  epicLog(LOG_WARNING, "end iteration thread1");
  delete wr;
}

//using wh2
void thread2() {
  epicLog(LOG_WARNING, "*****************start thread2**********************");
  int rg1, rg2, rg3, wbuf = 0x01010101;
  WorkRequest* wr = new WorkRequest();

  for (int i = 0; i < iteration; i++) {
    {
      //read g3
      rg3 = 0;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g3;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &rg3;
      if (wh2->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      } else {
        epicLog(LOG_INFO, "rg3 = %d", rg3);
      }
    }

    {
      //read g1
      rg1 = 0;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g1;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &rg1;
      if (wh2->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      } else {
        epicLog(LOG_INFO, "rg1 = %d", rg1);
      }
    }

    {
      //read g2
      rg2 = 0;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g2;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &rg2;
      if (wh2->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      } else {
        epicLog(LOG_INFO, "rg2 = %d", rg2);
      }
    }

    epicAssert(
        (rg2 >= rg1 && rg1 >= rg3) || rg1 == wbuf || rg2 == wbuf
            || rg3 == wbuf);
  }
  epicLog(LOG_WARNING, "end iteration thread2");
  delete wr;
}

//using wh3
void thread3() {
  epicLog(LOG_WARNING, "*****************start thread3**********************");
  int rg1, rg2, wbuf = 0x01010101;
  WorkRequest* wr = new WorkRequest();

  for (int i = 0; i < iteration; i++) {
    {
      //read g1
      rg1 = 0;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g1;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &rg1;
      if (wh3->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      } else {
        epicLog(LOG_INFO, "rg1 = %d", rg1);
      }
    }

    {
      //read g2
      rg2 = 0;
      memset(wr, 0, sizeof(WorkRequest));
      wr->addr = g2;
      wr->size = size;
      wr->flag = 0;
      wr->op = READ;
      wr->ptr = &rg2;
      if (wh3->SendRequest(wr)) {
        epicLog(LOG_WARNING, "send request failed");
      } else {
        epicLog(LOG_INFO, "rg2 = %d", rg2);
      }
    }

    epicAssert(rg2 >= rg1 || rg1 == wbuf || rg2 == wbuf);
  }
  epicLog(LOG_WARNING, "end iteration thread3");
  delete wr;
}

int main() {
  ibv_device **list = ibv_get_device_list(NULL);

  //master
  Conf* conf = new Conf();
  conf->loglevel = LOG_WARNING;
  GAllocFactory::SetConf(conf);
  Master* master = new Master(*conf);

  //worker1
  conf = new Conf();
  RdmaResource* res = new RdmaResource(list[0], false);
  Worker *worker1, *worker2, *worker3;
  worker1 = new Worker(*conf, res);
  wh1 = new WorkerHandle(worker1);

  //worker2
  conf = new Conf();
  res = new RdmaResource(list[0], false);
  conf->worker_port += 1;
  worker2 = new Worker(*conf, res);
  wh2 = new WorkerHandle(worker2);

  //worker3
  conf = new Conf();
  res = new RdmaResource(list[0], false);
  conf->worker_port += 2;
  worker3 = new Worker(*conf, res);
  wh3 = new WorkerHandle(worker3);

  char buf[size];
  char wbuf[size];
  WorkRequest wr { };

  //init the data to be written
  for (int i = 0; i < size; i++) {
    wbuf[i] = (char) i;
  }

  //w1 allocate
  wr.op = MALLOC;
  wr.size = size;
  if (wh1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  g1 = wr.addr;
  epicLog(LOG_WARNING, "\n****allocated g1 %ld at %lx*****\n", size, g1);

  //w2 allocate
  wr.addr = 0;
  if (wh2->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  g2 = wr.addr;
  epicLog(LOG_WARNING, "\n****allocated g2 %ld at %lx*****\n", size, g2);

  //w3 allocate
  wr.addr = 0;
  if (wh3->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
  g3 = wr.addr;
  epicLog(LOG_WARNING, "\n****allocated g3 %ld at %lx*****\n", size, g3);

//	//local write
//	wr.op = WRITE;
//	wr.addr = g1;
//	wr.size = size;
//	wr.ptr = wbuf;
//	if(wh1->SendRequest(&wr)) {
//		epicLog(LOG_WARNING, "send request failed");
//	}
//	wr.op = READ;
//	wr.ptr = buf;
//	if(wh1->SendRequest(&wr)) {
//		epicLog(LOG_WARNING, "send request failed");
//	}
//	for(i = 0; i < size; i++) {
//		if(buf[i] != wbuf[i]) {
//			epicLog(LOG_WARNING, "local read failed at buf[%d] (%d) != %d\n", i, buf[i], wbuf[i]);
//			break;
//		}
//	}
//	if(i == size) {
//		epicLog(LOG_WARNING, "local read succeed!");
//	} else {
//		epicLog(LOG_WARNING, "local read failed!");
//		exit(1);
//	}

  sleep(1);

  thread* t1 = new thread(thread1);
  thread* t2 = new thread(thread2);
  thread* t3 = new thread(thread3);

  t1->join();
  t2->join();
  t3->join();

  sleep(2);
  epicLog(LOG_WARNING, "test done");
  return 0;
}

