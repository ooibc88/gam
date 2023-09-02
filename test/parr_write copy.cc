#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>
#include <thread>
#include <pthread.h>
#include <complex>
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
#include "log.h"
#include "client.h"

using namespace std;
#define PI acos(-1)
#define N 64       // FFT点数
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
float xn[N];       // 采样信号序列

WorkerHandle *wh[10];
ibv_device **curlist;
Worker *worker[10];
Master *master;
int num_worker = 0;
int num_threads = 4;
int iteration_times = 0;
WorkerHandle *malloc_wh;
int parrallel_num = 2;

vector<pair<GAddr, int>> lock_list1;
vector<pair<GAddr, int>> lock_list2;
// map存储Workhandle到lock_list

void Create_master()
{
    Conf *conf = new Conf();
    // conf->master_port=12345;
    // conf->loglevel = LOG_DEBUG;
    conf->loglevel = LOG_TEST;
    GAllocFactory::SetConf(conf);
    master = new Master(*conf);
}

void Create_worker()
{
    Conf *conf = new Conf();
    RdmaResource *res = new RdmaResource(curlist[0], false);
    // conf->worker_port=12445;
    conf->worker_port += num_worker;
    worker[num_worker] = new Worker(*conf, res);
    wh[num_worker] = new WorkerHandle(worker[num_worker]);
    num_worker++;
}

void Read_val(WorkerHandle *Cur_wh, GAddr addr, int *val, int size)
{
    WorkRequest wr{};
    wr.op = READ;
    wr.wid = Cur_wh->GetWorkerId();
    wr.flag = 0;
    wr.size = size;
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void Write_val(WorkerHandle *Cur_wh, GAddr addr, int *val, int size)
{
    WorkRequest wr{};
    for (int i = 0; i < 1; i++)
    {
        wr.Reset();
        wr.op = WRITE;
        wr.wid = Cur_wh->GetWorkerId();
        // wr.flag = ASYNC; // 可以在这里调
        wr.size = size;
        wr.addr = addr;
        wr.ptr = (void *)val;
        if (Cur_wh->SendRequest(&wr))
        {
            epicLog(LOG_WARNING, "send request failed");
        }
    }
}

GAddr Malloc_addr(WorkerHandle *Cur_wh, const Size size, Flag flag, int Owner)
{
#ifdef LOCAL_MEMORY_HOOK
    void *laddr = zmalloc(size);
    return (GAddr)laddr;
#else
    WorkRequest wr = {};
    wr.op = MALLOC;
    wr.flag = flag;
    wr.size = size;
    wr.arg = Owner;

    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "malloc failed");
        return Gnullptr;
    }
    else
    {
        epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
        return wr.addr;
    }
#endif
}

void Free_addr(WorkerHandle *Cur_wh, GAddr addr)
{
    WorkRequest wr = {};
    wr.Reset();
    wr.addr = addr;
    wr.op = FREE;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void parrWrite1(WorkerHandle *Cur_wh, GAddr addr_xn, float *xn)
{
    for (int i = 0; i < N; i += 2)
    {
        Write_val(Cur_wh, addr_xn + i * sizeof(float), (int *)&xn[i], sizeof(float));
        // addr_xn + i * sizeof(float),sizeof(float)是一对值，需要添加到lock_list1
        lock_list1.push_back({addr_xn + i * sizeof(float), sizeof(float)});
    }
}

void parrWrite2(WorkerHandle *Cur_wh, GAddr addr_xn, float *xn)
{
    for (int i = 1; i < N; i += 2)
    {
        Write_val(Cur_wh, addr_xn + i * sizeof(float), (int *)&xn[i], sizeof(float));
        lock_list2.push_back({addr_xn + i * sizeof(float), sizeof(float)});
    }
}

int acquireLock(WorkerHandle *Cur_wh, GAddr addr, int size)
{

    return 0;
}
void flushToHome(WorkerHandle *Cur_wh, GAddr addr, int size)
{
    WorkRequest wr{};
    wr.op = flushToHomeOp;
    wr.wid = Cur_wh->GetWorkerId();
    wr.flag = size;
    wr.addr = addr;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}
int releaseLock(WorkerHandle *Cur_wh, GAddr addr)
{

    if (Cur_wh == wh[1])
    {
        // set<pair<GAddr, int>> s1(lock_list1.begin(), lock_list1.end());
        // lock_list1.assign(s1.begin(), s1.end());
        // 遍历lock_list1
        for (int i = 0; i < lock_list1.size(); i++)
        {
            GAddr addr = lock_list1[i].first;
            int size = lock_list1[i].second;
            // void *dest = malloc_wh->GetLocal(addr);
            // void *src = wh[1]->GetCacheLocal(addr);
            // printf("dest = %x, src = %x, addr = %x,size = %d\n", dest, src, addr, size);
            // wh[0]->FlushToHome(1, dest, src, size);
            flushToHome(wh[1], addr, size);
        }
    }
    if (Cur_wh == wh[2])
    {
        // set<pair<GAddr, int>> s2(lock_list2.begin(), lock_list2.end());
        // lock_list2.assign(s2.begin(), s2.end());
        for (int i = 0; i < lock_list2.size(); i++)
        {
            GAddr addr = lock_list2[i].first;
            int size = lock_list2[i].second;
            // void *dest = malloc_wh->GetLocal(addr);
            // void *src = wh[2]->GetCacheLocal(addr);
            // printf("dest = %x, src = %x, addr = %x,size = %d\n", dest, src, addr, size);
            // wh[2]->FlushToHome(1, dest, src, size);
            flushToHome(wh[2], addr, size);
        }
    }
    return 0;
}


void Solve()
{
    printf("Solve\n");
    Create_master();
    for (int i = 0; i < 3; ++i)
    {
        Create_worker();
    }
    malloc_wh = wh[0];

    sleep(1);

    float Xk[N];

    for (int i = 0; i < N; i++)
    {
        // xn[i] = 0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i);
        xn[i] = rand() % 100;
    }
    GAddr addr_xn = Malloc_addr(malloc_wh, sizeof(float) * N, RC_Write_shared, 1);
    // GAddr addr_xn = Malloc_addr(malloc_wh, sizeof(float) * N, Msi, 1);

    int Iteration = iteration_times;
    printf("Start\n");
    long Start = get_time();
    acquireLock(wh[1], addr_xn, N * sizeof(float));
    acquireLock(wh[2], addr_xn, N * sizeof(float));
    for (int round = 0; round < Iteration; ++round)
    {
        thread t1(parrWrite1, wh[1], addr_xn, xn);
        thread t2(parrWrite2, wh[2], addr_xn, xn); 
        t1.join();
        t2.join();
    }
    
    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);
    releaseLock(wh[1], addr_xn);
    releaseLock(wh[2], addr_xn);

    Read_val(wh[0], addr_xn, (int *)Xk, sizeof(float) * N);
    // compare xk xn
    for (int i = 0; i < N; i++)
    {
        if (Xk[i] != xn[i])
        {
            printf("Xk[%d] = %f, xn[%d] = %f\n", i, Xk[i], i, xn[i]);
        }
    }
}
void Solve2()
{
    printf("Solve2\n");
    Create_master();
    for (int i = 0; i < 3; ++i)
    {
        Create_worker();
    }
    malloc_wh = wh[0];

    sleep(1);

    float Xk[N];

    for (int i = 0; i < N; i++)
    {
        xn[i] = rand() % 100;
    }
    GAddr addr_xn = Malloc_addr(malloc_wh, sizeof(float) * N, Msi, 1);

    int Iteration = iteration_times;
    printf("Start\n");
    long Start = get_time();
    for (int round = 0; round < Iteration; ++round)
    {
        thread t1(parrWrite1, wh[1], addr_xn, xn);
        thread t2(parrWrite2, wh[2], addr_xn, xn);
        t1.join();
        t2.join();
    }
    

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);

    Read_val(wh[0], addr_xn, (int *)Xk, sizeof(float) * N);

    // compare xk xn
    for (int i = 0; i < N; i++)
    {
        if (Xk[i] != xn[i])
        {
            printf("Xk[%d] = %f, xn[%d] = %f\n", i, Xk[i], i, xn[i]);
        }
    }
}

void testFunc(){
    printf("testFunc\n");
    Create_master();
    for (int i = 0; i < 3; ++i)
    {
        Create_worker();
    }
    malloc_wh = wh[0];

    sleep(1);

    float writebuf[N], readbuf[N];
    for (int i = 0; i < N; i++)
    {
        writebuf[i] = rand() % 100;
    }
    

    GAddr addr = Malloc_addr(malloc_wh, sizeof(float) * N, RC_Write_shared, 1);
    
    for (int i = 0; i < N; i++)
    {
        int node_id = rand() % 3;
        Write_val(wh[node_id], addr + i * sizeof(float), (int *)&writebuf[i], sizeof(float));

        if(node_id == 1){
            lock_list1.push_back({addr + i * sizeof(float), sizeof(float)});
        }
        else if(node_id == 2){
            lock_list2.push_back({addr + i * sizeof(float), sizeof(float)});
        }
    }

    releaseLock(wh[1], addr);
    releaseLock(wh[2], addr);

    Read_val(wh[0], addr, (int *)readbuf, sizeof(float) * N);

    // compare xk xn

    for (int i = 0; i < N; i++)
    {
        printf("readbuf[%d] = %f, writebuf[%d] = %f\n", i, readbuf[i], i, writebuf[i]);
        
    }
    return;


}

int main(int argc, char *argv[])
{
    iteration_times = atoi(argv[1]);
    srand(time(NULL));
    curlist = ibv_get_device_list(NULL);

    Solve2();
    // testFunc();
    return 0;
}
