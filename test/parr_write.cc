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
#define N 64    // FFT点数
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
float xn[N];       // 采样信号序列
float Xk[N];

#define parrallel_num 2
WorkerHandle *wh[parrallel_num + 1];
ibv_device **curlist;
Worker *worker[parrallel_num + 1];
Master *master;
int num_worker = 0;
int iteration_times = 0;
int sub_iteration_times = 10;
WorkerHandle *malloc_wh;

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

void parrWrite(WorkerHandle *Cur_wh, GAddr addr_xn, float *xn, int start_index, int stride)
{
    for (int j = 0; j < sub_iteration_times; ++j)
    {
        for (int i = start_index; i < N; i += stride)
        {
            Write_val(Cur_wh, addr_xn + i * sizeof(float), (int *)&xn[i], sizeof(float));
        }
    }
}

void init_1()
{
    Create_master();
    for (int i = 0; i < parrallel_num + 1; ++i)
    {
        Create_worker();
    }
    malloc_wh = wh[0];

    sleep(1);
    for (int i = 0; i < N; i++)
    {
        xn[i] = rand() % 100;
    }
}

void Solve()
{
    // printf("Solve\n");

    GAddr addr_xn = Malloc_addr(malloc_wh, sizeof(float) * N, RC_Write_shared, 1);

    int Iteration = iteration_times;
    long Start = get_time();

    for (int i = 0; i < parrallel_num; i++)
    {
        wh[i + 1]->acquireLock(addr_xn, sizeof(float) * N);
    }
    thread t[parrallel_num];

    for (int round = 0; round < Iteration; ++round)
    {
        for (int i = 0; i < parrallel_num; i++)
        {
            t[i] = thread(parrWrite, wh[i + 1], addr_xn, xn, i, parrallel_num);
        }
        for (int i = 0; i < parrallel_num; i++)
        {
            t[i].join();
        }
    }

    long Mid = get_time();
    printf("%ld\n", Mid - Start);

    for (int i = 0; i < parrallel_num; i++)
    {
        wh[i + 1]->releaseLock(addr_xn);
    }

    Read_val(wh[0], addr_xn, (int *)Xk, sizeof(float) * N);
    // for (int i = 0; i < N; i++)
    // {
    //     printf("Xk[%d] = %f, xn[%d] = %f\n", i, Xk[i], i, xn[i]);
    // }

    long End = get_time();
    printf("%ld\n", End - Start);

    // for (int i = 0; i < parrallel_num; i++)
    // {
    //     wh[i + 1]->ReportCacheStatistics();
    //     wh[i + 1]->ResetCacheStatistics();
    // }
}

void Solve2()
{
    // printf("Solve2\n");

    GAddr addr_xn = Malloc_addr(malloc_wh, sizeof(float) * N, Msi, 1);

    int Iteration = iteration_times;
    long Start = get_time();
    thread t[parrallel_num];

    for (int round = 0; round < Iteration; ++round)
    {
        for (int i = 0; i < parrallel_num; i++)
        {
            t[i] = thread(parrWrite, wh[i + 1], addr_xn, xn, i, parrallel_num);
        }
        for (int i = 0; i < parrallel_num; i++)
        {
            t[i].join();
        }
    }
    Read_val(wh[0], addr_xn, (int *)Xk, sizeof(float) * N);
    // for (int i = 0; i < N; i++)
    // {
    //     printf("Xk[%d] = %f, xn[%d] = %f\n", i, Xk[i], i, xn[i]);
    // }

    long End = get_time();
    printf("%ld\n", End - Start);

    // for (int i = 0; i < parrallel_num; i++)
    // {
    //     wh[i + 1]->ReportCacheStatistics();
    //     wh[i + 1]->ResetCacheStatistics();
    // }
}

int main(int argc, char *argv[])
{
    iteration_times = atoi(argv[1]);
    srand(time(NULL));
    curlist = ibv_get_device_list(NULL);
    init_1();

    Solve();
    return 0;
}
