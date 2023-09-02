//
// Created by hrh on 1/4/23.
//
#include <iostream>
#include <gflags/gflags.h>

// GAM
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

using namespace std;
#define PI acos(-1)

typedef std::complex<float> Complex;

ibv_device **curlist;
Worker *worker[10];
Master *master;
int num_worker = 0;
WorkerHandle *malloc_wh;
WorkerHandle *wh[10];

// 可以改
int length = 1 << 6;
#define N length
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
int iteration_times = 1;
int no_run = 1;
int parrallel_num = 2;

void Create_master()
{
    Conf *conf = new Conf();
    // conf->loglevel = LOG_DEBUG;
    conf->loglevel = LOG_TEST;
    GAllocFactory::SetConf(conf);
    master = new Master(*conf);
}

void Create_worker()
{
    Conf *conf = new Conf();
    RdmaResource *res = new RdmaResource(curlist[0], false);
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

void Write_val(WorkerHandle *Cur_wh, GAddr addr, int *val, int size, int flush_id)
{
    WorkRequest wr{};
    if (Cur_wh->GetWorkerId() == 0)
    {
        flush_id = -1;
    }

    wr.Reset();
    wr.op = WRITE;
    wr.wid = Cur_wh->GetWorkerId();
    // wr.flag = ASYNC; // 可以在这里调
    wr.flush_id = flush_id;
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

void sub_fft(WorkerHandle *Cur_wh, GAddr addr_value, unsigned int n, unsigned int l, unsigned int k, Complex T)
{
    for (unsigned int a = l; a < N; a += n)
    {
        // a表示每一组的元素的下标，n表示跨度,k表示组数,l表示每一组的首个元素的下标
        unsigned int b = a + k;
        Complex xa, xb;
        Read_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
        Read_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
        Complex t = xa - xb;
        xa = xa + xb;
        Write_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
        xb = t * T;
        Write_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
    }
}
void test()
{
}

void p_fft_RC(GAddr addr_value)
{
    unsigned int k = N, n;
    float thetaT = PI / N;
    thread threads[parrallel_num];
    Complex phiT = Complex(cos(thetaT), -sin(thetaT));
    Complex T;

    while (k > 1)
    {
        printf("k = %d\n", k);
        // k=64,32,16,8,4,2,1 ，表示组数
        n = k;
        k >>= 1;
        if (k < parrallel_num)
            parrallel_num = k;
        phiT = phiT * phiT;
        T = 1.0L;
        // parallel

        for (int i = 0; i < parrallel_num; i++)
        {
            wh[i]->acquireLock(1, addr_value, sizeof(complex<float>) * N, true, sizeof(complex<float>));
        }

        for (unsigned int l = 0; l < k; l++)
        {

            // l表示每一组的首个元素的下标
            int id = l % parrallel_num;
            threads[id] = thread(sub_fft, wh[id], addr_value, n, l, k, T);
            threads[id].join();

            T *= phiT;
        }

        for (int i = 0; i < parrallel_num; i++)
            wh[i]->releaseLock(1, addr_value);


    }

    // Decimate
    unsigned int m = (unsigned int)log2(N);

    for (unsigned int a = 0; a < N; a++)
    {
        unsigned int b = a;
        // Reverse bits
        b = (((b & 0xaaaaaaaa) >> 1) | ((b & 0x55555555) << 1));
        b = (((b & 0xcccccccc) >> 2) | ((b & 0x33333333) << 2));
        b = (((b & 0xf0f0f0f0) >> 4) | ((b & 0x0f0f0f0f) << 4));
        b = (((b & 0xff00ff00) >> 8) | ((b & 0x00ff00ff) << 8));
        b = ((b >> 16) | (b << 16)) >> (32 - m);
        if (b > a)
        {
            // Complex t = x[a];
            // x[a] = x[b];
            // x[b] = t;
            Complex xa, xb, t;
            Read_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
            Read_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
            t = xa;
            xa = xb;
            xb = t;
            Write_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
            Write_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
        }
    }
}

void p_fft_MSI(GAddr addr_value)
{
    unsigned int k = N, n;
    float thetaT = PI / N;
    Complex phiT = Complex(cos(thetaT), -sin(thetaT));
    Complex T;
    thread threads[parrallel_num];

    while (k > 1)
    // while (k > N / 2)
    {
        // k=64,32,16,8,4,2,1 ，表示组数
        printf("k = %d\n", k);
        n = k;
        k >>= 1;
        if (k < parrallel_num)
            parrallel_num = k;
        phiT = phiT * phiT;
        T = 1.0L;

        // parallel

        for (unsigned int l = 0; l < k; l++)
        {

            // l表示每一组的首个元素的下标
            int id = l % parrallel_num;
            threads[id] = thread(sub_fft, wh[id], addr_value, n, l, k, T);
            // threads[id - 1] = thread(test);
            threads[id].join();

            T *= phiT;
        }
    }

    // Decimate
    unsigned int m = (unsigned int)log2(N);

    for (unsigned int a = 0; a < N; a++)
    {
        unsigned int b = a;
        // Reverse bits
        b = (((b & 0xaaaaaaaa) >> 1) | ((b & 0x55555555) << 1));
        b = (((b & 0xcccccccc) >> 2) | ((b & 0x33333333) << 2));
        b = (((b & 0xf0f0f0f0) >> 4) | ((b & 0x0f0f0f0f) << 4));
        b = (((b & 0xff00ff00) >> 8) | ((b & 0x00ff00ff) << 8));
        b = ((b >> 16) | (b << 16)) >> (32 - m);
        if (b > a)
        {
            // Complex t = x[a];
            // x[a] = x[b];
            // x[b] = t;
            Complex xa, xb, t;
            Read_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
            Read_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
            t = xa;
            xa = xb;
            xb = t;
            Write_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
            Write_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
        }
    }
}

void Solve_RC()
{
    malloc_wh = wh[0];

    complex<float> value[N];

    for (int i = 0; i < N; i++)
    {
        value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
        value[i].imag(0);
    }
    GAddr addr_value = Malloc_addr(malloc_wh, sizeof(complex<float>) * N, RC_Write_shared, 1);

    Write_val(malloc_wh, addr_value, (int *)value, sizeof(complex<float>) * N, 1);

    int Iteration = iteration_times;
    printf("Start\n");
    long Start = get_time();

    for (int round = 0; round < Iteration; ++round)
    {
        p_fft_RC(addr_value);
    }

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);

    complex<float> readbuf[N];
    for (int i = 0; i < N; i++)
    {
        Read_val(wh[0], addr_value + i * sizeof(complex<float>), (int *)&readbuf[i], sizeof(complex<float>));
        printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());
    }
}

void Solve_MSI()
{
    malloc_wh = wh[0];

    complex<float> value[N];

    for (int i = 0; i < N; i++)
    {
        value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
        value[i].imag(0);
    }
    GAddr addr_value = Malloc_addr(malloc_wh, sizeof(complex<float>) * N, Msi, 1);

    Write_val(malloc_wh, addr_value, (int *)value, sizeof(complex<float>) * N, 1);

    int Iteration = iteration_times;
    printf("Start\n");
    long Start = get_time();

    for (int round = 0; round < Iteration; ++round)
        p_fft_MSI(addr_value);

    complex<float> temp1;
    Read_val(wh[0], addr_value, (int *)&temp1, sizeof(complex<float>));
    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);

    complex<float> readbuf[N];
    Read_val(wh[0], addr_value, (int *)readbuf, sizeof(complex<float>) * N);
    for (int i = 0; i < N; i++)
        printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());

    for (int i = 0; i < num_worker; ++i)
        wh[i]->ReportCacheStatistics();
}


void Solve_WS()
{
    malloc_wh = wh[0];

    complex<float> value[N];

    for (int i = 0; i < N; i++)
    {
        value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
        value[i].imag(0);
    }
    GAddr addr_value = Malloc_addr(malloc_wh, sizeof(complex<float>) * N, Write_shared, 1);

    Write_val(malloc_wh, addr_value, (int *)value, sizeof(complex<float>) * N, 1);

    int Iteration = iteration_times;
    printf("Start\n");
    long Start = get_time();

    for (int round = 0; round < Iteration; ++round)
        p_fft_MSI(addr_value);

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);

    complex<float> readbuf[N];
    Read_val(wh[0], addr_value, (int *)readbuf, sizeof(complex<float>) * N);
    for (int i = 0; i < N; i++)
        printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());

    for (int i = 0; i < num_worker; ++i)
        wh[i]->ReportCacheStatistics();
}

int main(int argc, char *argv[])
{
    iteration_times = atoi(argv[1]);
    no_run = atoi(argv[2]);
    srand(time(NULL));
    curlist = ibv_get_device_list(NULL);
    Create_master();
    for (int i = 0; i < parrallel_num + 2; ++i)
        Create_worker();
    if (no_run == 1)
    {
        Solve_MSI();
    }
    else if (no_run == 2)
    {
        Solve_RC();
    }
    else if (no_run == 3)
    {
        Solve_WS();
    }
    return 0;
}