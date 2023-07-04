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

void Create_master()
{
    Conf *conf = new Conf();
    conf->loglevel = LOG_DEBUG;
    // conf->loglevel = LOG_TEST;
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
void dit_r2_fft(GAddr addr_xn, int n, int stride, GAddr addr_Xk, WorkerHandle *Cur_wh)
{
    complex<float> X1k[n / 2];
    complex<float> X2k[n / 2];
    GAddr addr_X1k = Malloc_addr(malloc_wh, sizeof(complex<float>) * n / 2, Msi, 1);
    GAddr addr_X2k = Malloc_addr(malloc_wh, sizeof(complex<float>) * n / 2, Msi, 1);

    if (n == 1)
    {
        complex<float> tmp;
        float xn_temp;
        Read_val(Cur_wh, addr_xn, (int *)&xn_temp, sizeof(float));
        tmp.real(xn_temp);
        tmp.imag(0);
        Write_val(Cur_wh, addr_Xk, (int *)&tmp, sizeof(complex<float>));
    }
    else
    {
        if (n >= N * 2 / parrallel_num)
        {
            // 0,2,4,..n-4,n-2;
            // 1,3,5,..n-3,n-1;

            thread t1(dit_r2_fft, addr_xn, n / 2, 2 * stride, addr_X1k, wh[1]);
            thread t2(dit_r2_fft, addr_xn + stride * sizeof(float), n / 2, 2 * stride, addr_X2k, wh[2]);
            t1.join();
            t2.join();
        }

        else
        {
            // 偶数n/2点DFT
            dit_r2_fft(addr_xn, n / 2, 2 * stride, addr_X1k, Cur_wh);
            // 奇数n/2点DFT
            dit_r2_fft(addr_xn + stride * sizeof(float), n / 2, 2 * stride, addr_X2k, Cur_wh);
        }

        // 蝶形运算
        for (int k = 0; k <= n / 2 - 1; k++)
        {
            complex<float> t;
            complex<float> WNk;
            complex<float> Xk_temp;

            WNk.real(cos(2 * PI / n * k));
            WNk.imag(-sin(2 * PI / n * k));

            Read_val(Cur_wh, addr_X2k + k * sizeof(complex<float>), (int *)&X2k[k], sizeof(complex<float>));
            Read_val(Cur_wh, addr_X1k + k * sizeof(complex<float>), (int *)&X1k[k], sizeof(complex<float>));

            t = WNk * X2k[k];
            Xk_temp = X1k[k] + t;
            Write_val(Cur_wh, addr_Xk + k * sizeof(complex<float>), (int *)&Xk_temp, sizeof(complex<float>));
            Xk_temp = X1k[k] - t;
            Write_val(Cur_wh, addr_Xk + (k + n / 2) * sizeof(complex<float>), (int *)&Xk_temp, sizeof(complex<float>));
        }
    }

    Free_addr(Cur_wh, addr_X1k);
    Free_addr(Cur_wh, addr_X2k);
}

void sub_fft2()
{

}
void p_fft2(GAddr addr_input, int n, int stride, GAddr addr_output, WorkerHandle *Cur_wh)
{
    for (int i = 0; i < n; i++)
    {
        complex<float> temp1[N];
        Read_val(Cur_wh, addr_input + i * sizeof(complex<float>), (int *)&temp1, sizeof(complex<float>) * N);
        Write_val(Cur_wh, addr_output + i * sizeof(complex<float>), (int *)&temp1, sizeof(complex<float>) * N);
    }
    
}

void Solve2()
{
    malloc_wh = wh[0];

    complex<float> value_input[N];

    for (int i = 0; i < N; i++)
    {
        value_input[i] = 0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i);
    }
    GAddr addr_input = Malloc_addr(malloc_wh, sizeof(float) * N, Msi, 1);
    for (int i = 0; i < N; i++)
    {
        Write_val(malloc_wh, addr_input + i * sizeof(float), (int *)&xn[i], sizeof(float));
    }

    GAddr addr_output = Malloc_addr(malloc_wh, sizeof(complex<float>) * N, Msi, 1);

    int Iteration = iteration_times;
    printf("Start\n");
    long Start = get_time();
    for (int round = 0; round < Iteration; ++round)
    {
        p_fft2(addr_input, N, 1, addr_output, malloc_wh);
    }

    // for(int i=0;i<N;i++)
    // {
    //     Read_val(wh[0], addr_Xk + i * sizeof(complex<float>), (int *)&Xk[i], sizeof(complex<float>));
    //     printf("Xk[%d]=%f+%fi\n",i,Xk[i].real(),Xk[i].imag());
    // }

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);
}

void Solve()
{
    malloc_wh = wh[0];

    sleep(1);

    complex<float> Xk[N];

    for (int i = 0; i < N; i++)
    {
        xn[i] = 0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i);
    }
    GAddr addr_xn = Malloc_addr(malloc_wh, sizeof(float) * N, Msi, 1);
    for (int i = 0; i < N; i++)
    {
        Write_val(malloc_wh, addr_xn + i * sizeof(float), (int *)&xn[i], sizeof(float));
    }

    GAddr addr_Xk = Malloc_addr(malloc_wh, sizeof(complex<float>) * N, Msi, 1);

    int Iteration = iteration_times;
    printf("Start\n");
    long Start = get_time();
    for (int round = 0; round < Iteration; ++round)
    {
        dit_r2_fft(addr_xn, N, 1, addr_Xk, malloc_wh);
    }

    // for(int i=0;i<N;i++)
    // {
    //     Read_val(wh[0], addr_Xk + i * sizeof(complex<float>), (int *)&Xk[i], sizeof(complex<float>));
    //     printf("Xk[%d]=%f+%fi\n",i,Xk[i].real(),Xk[i].imag());
    // }

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);
}

int main(int argc, char *argv[])
{
    iteration_times = atoi(argv[1]);
    srand(time(NULL));
    curlist = ibv_get_device_list(NULL);
    Create_master();
    for (int i = 0; i < 3; ++i)
    {
        Create_worker();
    }

    Solve();
    return 0;
}
