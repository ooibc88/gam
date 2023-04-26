
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

WorkerHandle *wh1_1, *wh1_2, *wh2_1, *wh2_2, *wh3_1, *wh3_2;
Size size = BLOCK_SIZE;  //eight blocks
GAddr g1, g2, g3;
char local_init = 1;
char remote_init = 2;
int MAX_CASES = 100;
int ng1 = 1000, ng2 = 2000, ng3 = 1000, ng4 = 1000;

void run_for_test() {
  int aa;
  int bb;
}

int Write_buf[100010];
int Read_buf[100010];

int main() {
  srand(time(NULL));
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

  WorkRequest wr { };
  wr.Reset();
  wr.op = MALLOC;
  wr.size = sizeof(int) * 100000;
  wr.flag = (int)(Access_exclusive);
  wr.arg = 2ull;

  if (wh1_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }

  printf ("Got here\n");

  GAddr A = wr.addr;
  //多个单位测试
  {
  
    

    memset(Read_buf, 0, sizeof(Read_buf));
    memset(Write_buf, 0, sizeof(Write_buf));
/*
    for (int i = 0; i < 128; ++i) {
      if (i < 100) Write_buf[i] = i;
      else Write_buf[i] = 888;
    }
    */
    
    //随机用3个节点去写某个位置，最后sleep(1)之后随机用三个节点去读到read_buf，看是否与write_buf相同
    for (int i = 0; i < 100000; ++i) {
      int x = (int)rand() % 3;
      
      int val = (int)rand();
      Write_buf[i] = val;
      wr.Reset();
      wr.op = WRITE;
      wr.flag = ASYNC;
      wr.size = sizeof(int);
      wr.addr = A + i * sizeof(int);
      wr.ptr = (void*)(&val);
      if (x == 2) {
        if (wh3_1->SendRequest(&wr)) {
          epicLog(LOG_WARNING, "send request failed");
        }
      }
      else if (x == 1) {
        if (wh2_1->SendRequest(&wr)) {
          epicLog(LOG_WARNING, "send request failed");
        }
      }
      else {
        if (wh1_1->SendRequest(&wr)) {
          epicLog(LOG_WARNING, "send request failed");
        }
      }
    }

    sleep(2);

    for (int i = 0; i < 100000; ++i) {
      int x = (int)rand() % 3;
      
      int val = 0;
      wr.Reset();
      wr.op = READ;
      wr.flag = 0;
      wr.size = sizeof(int);
      wr.addr = A + i * sizeof(int);
      wr.ptr = (void*)(&val);
      if (x == 2) {
        if (wh3_1->SendRequest(&wr)) {
          epicLog(LOG_WARNING, "send request failed");
        }
      }
      else if (x == 1) {
        if (wh2_1->SendRequest(&wr)) {
          epicLog(LOG_WARNING, "send request failed");
        }
      }
      else {
        if (wh1_1->SendRequest(&wr)) {
          epicLog(LOG_WARNING, "send request failed");
        }
      }
      Read_buf[i] = val;
    }
    // 一次性写128个位置，看能不能读到
    /*
    {
      wr.Reset();
      wr.op = WRITE;
      wr.flag = ASYNC;
      wr.size = sizeof(int) * 128;
      wr.addr = A;
      wr.ptr = (void*)Write_buf;
      if (wh3_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
      
      sleep(1);

      int Read_buf[128];
      wr.Reset();
      wr.op = READ;
      wr.flag = 0;
      wr.size = sizeof(int) * 128;
      wr.addr = A;
      int Read_Cur = 0;
      wr.ptr = (void*)(Read_buf);
      if (wh1_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }
    */
      
     
    for (int i = 64; i < 188; ++i) {
      printf ("Read_buf[%d]=%d, Write_buf[%d]=%d\n", i, Read_buf[i], i, Write_buf[i]);
    }

    for (int i = 0; i < 100000; ++i) {
      if (Read_buf[i] != Write_buf[i]) {
        printf ("Error on %d\n", i);
      }
    }
    
  }
  /*
  //先检验1号节点写，1->2->write，2号节点读是否能读到新值,单个单位测试
  {
    wr.Reset();
    wr.op = WRITE;
    wr.flag = ASYNC;
    wr.size = sizeof(int);
    wr.addr = A;
    int Write_Cur = 12363164;
    wr.ptr = (void*)(&Write_Cur);
    if (wh3_1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    sleep(1);

    wr.Reset();
    wr.op = READ;
    wr.flag = 0;
    wr.size = sizeof(int);
    wr.addr = A;
    int Read_Cur = 0;
    wr.ptr = (void*)(&Read_Cur);
    printf ("current Cur : %d\n", Read_Cur);
    if (wh1_1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    printf ("current Cur : %d\n", Read_Cur);
  }
  */

/*
  for (int i = 1; i <= 20; ++i) {
    wr.Reset();
    if (!(i & 1)) {
      wr.op = READ;
      printf ("Read ");
    }
    else {
      wr.op = WRITE, wr.flag |= ASYNC;
      printf ("Write %d", i); 
    }
    wr.size = sizeof(int);
    wr.addr = A;

    int Cur = i;

    wr.ptr = (void *)(&Cur);

    if (i % 10 == 0) {
      if (wh3_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }

    else {
      if (wh1_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
      }
    }

    printf ("Round %d, Cur : %d\n", i, Cur);
  }
*/
  return 0;
}