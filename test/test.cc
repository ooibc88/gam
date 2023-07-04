
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
}

WorkerHandle * wh[1000];
ibv_device **curlist;
Worker * worker[100];
Master * master;
int num_worker = 0;

int Write_buf[100010];
int Read_buf[100010];
int Man[100010];
int W[100010];

void Send_Fence (WorkerHandle * Cur_wh) {
  WorkRequest wr { };
  wr.flag = ASYNC;
  wr.op = MFENCE;
  wr.flag = ASYNC;
  wr.ptr = nullptr;
  if (Cur_wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
}

void Create_master() {
  Conf* conf = new Conf();
  conf->loglevel = LOG_WARNING;
  //conf->loglevel = LOG_DEBUG;
  GAllocFactory::SetConf(conf);
  master = new Master(*conf);
}

void Create_worker() {
  Conf* conf = new Conf();
  RdmaResource* res = new RdmaResource(curlist[0], false);
  conf->worker_port += num_worker;
  worker[num_worker] = new Worker(*conf, res);
  wh[num_worker] = new WorkerHandle(worker[num_worker]);
  num_worker ++;
}

GAddr Malloc_addr(WorkerHandle * Cur_wh, const Size size, Flag flag, int Owner) {
#ifdef LOCAL_MEMORY_HOOK
  void* laddr = zmalloc(size);
  return (GAddr)laddr;
#else
  WorkRequest wr = { };
  wr.op = MALLOC;
  wr.flag = flag;
  wr.size = size;
  /* add ergeda add */
  wr.arg = (uint64_t) Owner; //arg没啥用，刚好用来存储owner
  /* add ergeda add */

  if (Cur_wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "malloc failed");
    return Gnullptr;
  } else {
    epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
    return wr.addr;
  }
#endif
}

void Read_val (WorkerHandle * Cur_wh, GAddr addr, int * val) {
  WorkRequest wr { };
  wr.op = READ;
  wr.flag = 0;
  wr.size = sizeof(int);
  wr.addr = addr;
  wr.ptr = (void*)val;
  if (Cur_wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
}

void Write_val (WorkerHandle * Cur_wh, GAddr addr, int * val) {
  WorkRequest wr { };
  wr.op = WRITE;
  wr.flag = ASYNC; //可以在这里调
  wr.size = sizeof(int);
  wr.addr = addr;
  wr.ptr = (void*)val;
  if (Cur_wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
}

void ProcessRead (WorkerHandle * Cur_wh, GAddr addr, int * val) {
  Read_val (Cur_wh, addr, val);
}

void ProcessRead_Write (WorkerHandle * Cur_wh, GAddr addr, GAddr addr2, int * val) {
  Read_val (Cur_wh, addr, val);
  Write_val (Cur_wh, addr2, val);
  Read_val(Cur_wh, addr2, val);
}

atomic<int> Count;

void PrivateRead(WorkerHandle * Cur_wh, GAddr addr, int * val, int num) {
  while (num --) {
    int x = rand() % 4;
    //if (x == 3) Read_val(Cur_wh, addr, val);
    if (x != 0) Read_val(Cur_wh, addr, val);
    else {
      Write_val(Cur_wh, addr, val);
      Send_Fence(Cur_wh);
    }
  }
}

void Random_read(WorkerHandle * Cur_wh, GAddr addr, int * val, int num) {
  while (num --) {
    int x = rand() % 2;
    if (x == 0) Read_val(Cur_wh, addr, val);
    else {
      Write_val(Cur_wh, addr, val);
    }
  }
}

void Test_Portion() { //随机的读写，读写比例，节点自定义
  Count = 0;
  //GAddr addr = Malloc_addr(wh[1], sizeof(int), 0, 1);
  GAddr addr = Malloc_addr(wh[2], BLOCK_SIZE, Access_exclusive, 3);  
  printf ("addr : %lld\n", addr);
  // Start iteration
  int Iteration = 100000;
  printf ("Start\n");
  long Start = get_time();
  for (int round = 0; round < Iteration; ++ round) {
    int val = rand();
    std::list<std::thread*> threads;
    for (int i = 0; i < num_worker; ++i) {
       // 创建一个存储 std::thread 指针的容器
      std::thread* thread_new = new std::thread(PrivateRead, wh[i], addr, &val, i == 2 ? 50 : 10);
      threads.push_back(thread_new);
      
      //Read_val (wh[i], addr, &val);
      /*if (val != Lastval) {
        printf ("node : %d, val : %d, Lastval : %d\n", i+1, val, Lastval);
        flag = 1;
      }*/
    }
    for (auto thread : threads) { thread->join(); } //阻塞主线程等待子线程执行完毕
    for (auto thread : threads) { delete thread; }
  }

  printf ("read portion : %.5lf\n", 1.0 * 100000 / (1.0 * Count) );

  long End = get_time();
  printf ("End\n");
  printf ("running time : %lld\n", End - Start);
}

void Test_random() { //随机的读写，读写比例，节点自定义
  Count = 0;
  GAddr addr = Malloc_addr(wh[1], BLOCK_SIZE, Write_exclusive, 3);  
  printf ("addr : %lld\n", addr);
  // Start iteration
  int Iteration = 10000;
  printf ("Start\n");
  long Start = get_time();
  for (int round = 0; round < Iteration; ++ round) {
    int val = rand();
    std::list<std::thread*> threads;
    for (int i = 0; i < num_worker; ++i) {
       // 创建一个存储 std::thread 指针的容器
      std::thread* thread_new = new std::thread(Random_read, wh[i], addr, &val, 5);
      threads.push_back(thread_new);
      
      //Read_val (wh[i], addr, &val);
      /*if (val != Lastval) {
        printf ("node : %d, val : %d, Lastval : %d\n", i+1, val, Lastval);
        flag = 1;
      }*/
    }
    for (auto thread : threads) { thread->join(); } //阻塞主线程等待子线程执行完毕
    for (auto thread : threads) { delete thread; }
  }

  printf ("read portion : %.5lf\n", 1.0 * 100000 / (1.0 * Count) );

  long End = get_time();
  printf ("End\n");
  printf ("running time : %lld\n", End - Start);
}

void Test_Communicate() {
  GAddr addr = Malloc_addr(wh[1], BLOCK_SIZE, Access_exclusive, 2); 
  printf ("addr : %lld\n", addr);
  // Start iteration
  int Iteration = 100000;
  printf ("Start\n");
  long Start = get_time();
  for (int round = 0; round < Iteration; ++ round) {
    int val = rand();
    Write_val(wh[1], addr, &val);
  }
  //144069640
  //27849288
  long End = get_time();
  printf ("End\n");
  printf ("running time : %lld\n", End - Start);
}

void Test_singleblock() {
  GAddr addr = Malloc_addr(wh[1], BLOCK_SIZE, Write_exclusive, 2); 
  int val = rand();
  Write_val(wh[0], addr, &val); 
  printf ("write val : %d\n", val);
  int Lastval = val;
  Send_Fence(wh[0]);
  bool flag = 0;
  //std::list<std::thread*> threads;
  sleep(1);
  Read_val (wh[1], addr, &val);
  printf ("read val : %d\n", val);
}

void Solve (){
  Create_master();
  for (int i = 0; i < 10; ++i) {
    Create_worker();
  }

  //sleep(1);
  //Test_Communicate();
  Test_Portion();
  //Test_random();
  //return;
  //
  //Test_singleblock();
  return;

  GAddr addr = Malloc_addr(wh[1], BLOCK_SIZE, Write_exclusive, 3);  
  //GAddr addr2 = Malloc_addr(wh[1], BLOCK_SIZE, 0, 2);
  printf ("addr : %lld\n", addr);
  //printf ("addr2 : %lld\n", addr2);
  // Start iteration
  int Iteration = 100000;
  printf ("Start\n"); 
  long Start = get_time(); 
  for (int round = 0; round < Iteration; ++ round) {
    //printf ("Got to Round %d\n", round);
    int x = rand() % num_worker;
    int val = rand();
    Write_val(wh[0], addr, &val); 
    int Lastval = val;
    Send_Fence(wh[0]);
    bool flag = 0;
    //std::list<std::thread*> threads;
    Read_val (wh[0], addr, &val);
    for (int i = 1; i < num_worker; ++i) {
       // 创建一个存储 std::thread 指针的容器
      //std::thread* thread_new = new std::thread(ProcessRead, wh[i], addr, &val);
      //std::thread* thread_new = new std::thread(ProcessRead_Write, wh[i], addr, addr2, &val);
      //threads.push_back(thread_new);
      
      Read_val (wh[i], addr, &val);
      if (val != Lastval) {
        printf ("node : %d, val : %d, Lastval : %d\n", i+1, val, Lastval);
        flag = 1;
      }
    }
    //for (auto thread : threads) { thread->join(); } //阻塞主线程等待子线程执行完毕
    //for (auto thread : threads) { delete thread; }
    if (flag) {
      printf ("error on round %d\n", round);
      //return;
    }
  }

  for (int i = 0; i < num_worker; ++i) {
    Send_Fence (wh[i]);
  }
  long End = get_time();
  printf ("End\n");
  printf ("running time : %lld\n", End - Start);
  // End iteration
} //14083322110 serial 10/100000
  //64265708711 serial 10/100000

  //30160532516 muiti 10/100000
  //31249869840 muiti 10/100000

  //64251493096 muiti 20/100000
  //64359665663 muiti 20/100000
  

int main() {
  //FILE * fp = fopen("log_test.txt", "w");
  srand(time(NULL));
  curlist = ibv_get_device_list(NULL);
  //ibv_device **list = ibv_get_device_list(NULL);;
  int i;

  // Solve();
  // return 0;
  /*
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
  wr.flag = (int)(Read_mostly);
  wr.arg = 0ull; //

  if (wh2_1->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }

  printf ("Got here\n");

  GAddr A = wr.addr;
  */
  /* test rdma */
  /*
  printf ("\n\n\n\n"); 
  worker1->TestSend(A);
  while(1) {} 
  return 0;
  */  
  /* test rdma */
  //多个单位测试
  {
  
    
/*
    memset(Read_buf, 0, sizeof(Read_buf));
    memset(Write_buf, 0, sizeof(Write_buf));
    */
/*
    for (int i = 0; i < 128; ++i) {
      if (i < 100) Write_buf[i] = i;
      else Write_buf[i] = 888;
    }
    */
    
    //随机用3个节点去写某个位置，最后sleep(1)之后随机用三个节点去读到read_buf，看是否与write_buf相同
    /*
    for (int i = 0; i < 100000; ++i) {
      int x = (int)rand() % 3;
      int val = (int)rand();
      Man[i] = x;
      W[i] = val;
      fprintf (fp, "worker %d write %d\n", x+1, val); 
      fflush(fp);
    }

    for (int i = 0; i < 100000; ++i) {
      int x = Man[i];
      int val = W[i];

      Write_buf[i] = val;
      wr.Reset();
      wr.op = WRITE;
      wr.flag = ASYNC; //可以在这里调
      wr.size = sizeof(int);
      wr.addr = A + i * sizeof(int);
      wr.ptr = (void*)(&val);
      //fprintf (fp, "worker %d write %d\n", x+1, val); 
      //fflush(fp);
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
    

    //sleep(2);
    Send_Fence (wh1_1);
    Send_Fence (wh2_1);
    Send_Fence (wh3_1);
    printf ("Write already done haha!\n");

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
      if (Read_buf[i] != Write_buf[i]) {
        printf ("worker : %d, Error on %d, write : %d, read : %d\n", x + 1, i, Write_buf[i], Read_buf[i]);
      }
    }

    printf ("Read already done\n");
    */
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
      
    /*
    for (int i = 2346; i < 2390; ++i) {
      printf ("Read_buf[%d]=%d, Write_buf[%d]=%d\n", i, Read_buf[i], i, Write_buf[i]);
    }

    for (int i = 0; i < 100000; ++i) {
      //if (Read_buf[i] != Write_buf[i]) {
        //printf ("Error on %d, write : %d, read : %d\n", i, Write_buf[i], Read_buf[i]);
      //}
    }
    */
  }
  
  //先检验1号节点写，1->2->write，2号节点读是否能读到新值,单个单位测试
  /*
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
    int adsgasg;

    wr.Reset();
    wr.op = WRITE;
    wr.flag = ASYNC;
    wr.size = sizeof(int);
    wr.addr = A;
    Write_Cur = 234756;
    wr.ptr = (void*)(&Write_Cur);
    if (wh1_1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }

    sleep(1);

    wr.Reset();
    wr.op = WRITE;
    wr.flag = ASYNC;
    wr.size = sizeof(int);
    wr.addr = A;
    Write_Cur = 436756;
    wr.ptr = (void*)(&Write_Cur);
    if (wh2_1->SendRequest(&wr)) {
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
    if (wh3_1->SendRequest(&wr)) {
      epicLog(LOG_WARNING, "send request failed");
    }
    
    printf ("current Cur : %d\n", Read_Cur);
  }
  */
  ////

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
  //fclose(fp);
  return 0;
}

/*
cache.lock(wr->addr);
  GAddr pend = GADD(parent->addr, parent->size);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
  void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
  void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
  Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
  memcpy(ls, cs, len);

  
  CacheLine * cline = cache.GetCLine(wr->addr);
  cline->state = CACHE_SHARED;
  cache.unlock(wr->addr);

  if ( (-- parent->counter) == 0) {
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
*/