// GAM
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <thread>
#include <pthread.h>
#include <complex>
#include <cstring>
#include <thread>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"
#include <string>
#include <cstring>
#include <iostream>
#include "gallocator.h"
#include "log.h"
#include <cstring>

#define DEBUG_LEVEL LOG_TEST
#define STEPS 204800
#define SYNC_KEY STEPS

using namespace std;
#define PI acos(-1)

typedef std::complex<float> Complex;

// 可以改
int length = 1 << 10;
#define N length
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
int iteration_times = 1;

int node_id;
int id;
int no_node = 0;
int is_read = 0;
int is_sync = 0;
int no_run = 0;
int see_time = 0;
double sleep_time = 0.0;

int parrallel_num = 0;
int is_master = 1;
string ip_master = get_local_ip("eth0");
string ip_worker = get_local_ip("eth0");
int port_master = 12345;
int port_worker = 12346;
GAlloc *alloc;

void sub_fft(GAddr addr_value, unsigned int n, unsigned int l, unsigned int k, Complex T)
{
    for (unsigned int a = l; a < N; a += n)
    {
        // a表示每一组的元素的下标，n表示跨度,k表示组数,l表示每一组的首个元素的下标
        unsigned int b = a + k;
        Complex xa, xb;
        // Read_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
        alloc->Read(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
        // Read_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
        alloc->Read(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
        Complex t = xa - xb;
        xa = xa + xb;
        // Write_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
        alloc->Write(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
        xb = t * T;
        // Write_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
        alloc->Write(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
    }
}

void p_fft_RC(GAddr addr_value)
{
    unsigned int k = N, n;
    float thetaT = PI / N;
    Complex phiT = Complex(cos(thetaT), -sin(thetaT));
    Complex T;
    parrallel_num = no_node;

    while (k > 1)
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
        if (no_run == 2)
        {
            alloc->acquireLock(1, addr_value, sizeof(complex<float>) * N, true, sizeof(complex<float>));
        }

        // sync
        for (unsigned int l = 0; l < k; l++)
        {
            int id_parr = l % parrallel_num + 1;
            if (id_parr == node_id)
            {
                sub_fft(addr_value, n, l, k, T);
            }
            T *= phiT;
        }
        if (no_run == 2)
        {
            alloc->releaseLock(1, addr_value);
        }

        id = 0;
        alloc->Put(SYNC_KEY * 4 + node_id + k * 100, &node_id, sizeof(int));
        for (int i = 1; i <= no_node; i++)
        {
            alloc->Get(SYNC_KEY * 4 + i + k * 100, &id);
            epicAssert(id == i);
        }
    }

    if (is_master)
    {
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
                // Read_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                alloc->Read(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                // Read_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                alloc->Read(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                t = xa;
                xa = xb;
                xb = t;
                // Write_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
                alloc->Write(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
                // Write_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
                alloc->Write(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
            }
        }
    }
}

void p_fft_MSI(GAddr addr_value)
{
    unsigned int k = N, n;
    float thetaT = PI / N;
    Complex phiT = Complex(cos(thetaT), -sin(thetaT));
    Complex T;
    parrallel_num = no_node;

    while (k > 1)
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
            int id_parr = l % parrallel_num + 1;
            if (id_parr == node_id)
            {
                sub_fft(addr_value, n, l, k, T);
            }
            T *= phiT;
        }

        id = 0;
        alloc->Put(SYNC_KEY * 4 + node_id + k * 100, &node_id, sizeof(int));
        for (int i = 1; i <= no_node; i++)
        {
            alloc->Get(SYNC_KEY * 4 + i + k * 100, &id);
            epicAssert(id == i);
        }
    }

    // Decimate
    if (is_master)
    {
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
                // Read_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                alloc->Read(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                // Read_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                alloc->Read(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                t = xa;
                xa = xb;
                xb = t;
                // Write_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
                alloc->Write(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                // Write_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
                alloc->Write(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
            }
        }
    }
}

void Solve_RC()
{
    GAddr addr_value;
    long Start;
    long End;
    if (is_master)
    {
        complex<float> value[N];

        for (int i = 0; i < N; i++)
        {
            value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
            value[i].imag(0);
        }
        addr_value = alloc->AlignedMalloc(sizeof(complex<float>) * N, RC_Write_shared, 1);
        printf("addr_value=%lx\n", addr_value);
        alloc->Write(addr_value, (int *)value, sizeof(complex<float>) * N);
        alloc->Put(141, &addr_value, sizeof(long long));
    }
    else
    {
        printf("begin slave\n");
        alloc->Get(141, &addr_value);
        printf("addr_value=%lx\n", addr_value);
    }
    id = 0;
    alloc->Put(SYNC_KEY * 2 + node_id, &node_id, sizeof(int));
    for (int i = 1; i <= no_node; i++)
    {
        alloc->Get(SYNC_KEY * 2 + i, &id);
        epicAssert(id == i);
    }

    if (is_master)
    {
        printf("Start\n");
        Start = get_time();
    }

    p_fft_RC(addr_value);

    if (is_master)
    {
        End = get_time();
        printf("End\n");
        printf("running time : %ld\n", End - Start);
        if (is_read)
        {
            complex<float> readbuf[N];
            alloc->Read(addr_value, (int *)readbuf, sizeof(complex<float>) * N);

            printf("\n\nresult:\n");
            for (int i = 0; i < 10; i++)
                printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());
        }
    }
}

void Solve_MSI()
{
    GAddr addr_value;
    long Start;
    long End;
    if (is_master)
    {
        complex<float> value[N];

        for (int i = 0; i < N; i++)
        {
            value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
            value[i].imag(0);
        }
        addr_value = alloc->AlignedMalloc(sizeof(complex<float>) * N, Msi, 1);
        printf("addr_value=%lx\n", addr_value);
        alloc->Write(addr_value, (int *)value, sizeof(complex<float>) * N);
        alloc->Put(141, &addr_value, sizeof(long long));
    }
    else
    {
        printf("begin slave\n");
        alloc->Get(141, &addr_value);
        printf("addr_value=%lx\n", addr_value);
    }

    alloc->Put(SYNC_KEY * 2 + node_id, &node_id, sizeof(int));
    for (int i = 1; i <= no_node; i++)
    {
        alloc->Get(SYNC_KEY * 2 + i, &id);
        epicAssert(id == i);
    }

    if (is_master)
    {
        Start = get_time();
    }

    p_fft_MSI(addr_value);

    if (is_master)
    {
        End = get_time();
        printf("End\n");
        printf("running time : %ld\n", End - Start);
        if (is_read)
        {
            complex<float> readbuf[N];
            alloc->Read(addr_value, (int *)readbuf, sizeof(complex<float>) * N);

            printf("\n\nresult:\n");
            for (int i = 0; i < 10; i++)
                printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());
        }
    }

    alloc->ReportCacheStatistics();
}

void func()
{
    printf("bad!,master first come in\n");
}

int main(int argc, char *argv[])
{

    for (int i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--ip_master") == 0)
        {
            ip_master = string(argv[++i]);
        }
        else if (strcmp(argv[i], "--ip_worker") == 0)
        {
            ip_worker = string(argv[++i]);
        }
        else if (strcmp(argv[i], "--port_master") == 0)
        {
            port_master = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--iface_master") == 0)
        {
            ip_master = get_local_ip(argv[++i]);
        }
        else if (strcmp(argv[i], "--port_worker") == 0)
        {
            port_worker = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--iface_worker") == 0)
        {
            ip_worker = get_local_ip(argv[++i]);
        }
        else if (strcmp(argv[i], "--iface") == 0)
        {
            ip_worker = get_local_ip(argv[++i]);
            ip_master = get_local_ip(argv[i]);
        }
        else if (strcmp(argv[i], "--is_master") == 0)
        {
            is_master = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--no_node") == 0)
        {
            no_node = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--no_run") == 0)
        {
            no_run = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--is_read") == 0)
        {
            is_read = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--is_sync") == 0)
        {
            is_sync = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--see_time") == 0)
        {
            see_time = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--sleep_time") == 0)
        {
            sleep_time = atoi(argv[++i]);
        }
        else
        {
            fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
        }
    }

    Conf conf;
    conf.loglevel = DEBUG_LEVEL;
    conf.is_master = is_master;
    conf.master_ip = ip_master;
    conf.master_port = port_master;
    conf.worker_ip = ip_worker;
    conf.worker_port = port_worker;

    cout << "is_master=" << is_master << ",master_ip=" << ip_master << ",master_port=" << port_master << ",worker_ip=" << ip_worker << ",worker_port=" << port_worker << endl;

    alloc = GAllocFactory::CreateAllocator(&conf);

    node_id = alloc->GetID();
    alloc->Put(SYNC_KEY + node_id, &node_id, sizeof(int));
    for (int i = 1; i <= no_node; i++)
    {
        alloc->Get(SYNC_KEY + i, &id);
        epicAssert(id == i);
    }

    if (no_run == 1)
    {
        Solve_MSI();
    }
    else if (no_run >= 2)
    {
        Solve_RC();
    }
    return 0;
}