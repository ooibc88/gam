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

WorkerHandle *wh1_1, *wh1_2, *wh2_1, *wh2_2, *wh3_1, *wh3_2;
Size size = BLOCK_SIZE; // 512byte
GAddr g1, g2, g3;
char local_init = 1;
char remote_init = 2;
int MAX_CASES = 100;
int ng1 = 1000, ng2 = 2000, ng3 = 1000, ng4 = 1000;
GAddr addr, addr1, addr2, addr3;

void run_for_test()
{
    int aa;
}

WorkerHandle *wh[1000];
ibv_device **curlist;
Worker *worker[100];
Master *master;
int num_worker = 0;

int Write_buf[100010];
int Read_buf[100010];
int Man[100010];
int W[100010];

void Send_Fence(WorkerHandle *Cur_wh)
{
    WorkRequest wr{};
    wr.flag = ASYNC;
    wr.op = MFENCE;
    wr.flag = ASYNC;
    wr.ptr = nullptr;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void Create_master()
{
    Conf *conf = new Conf();
    conf->loglevel = LOG_PQ;
    // conf->loglevel = LOG_TEST;
    // conf->loglevel = LOG_WARNING;
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

void Read_val(WorkerHandle *Cur_wh, GAddr addr, int *val)
{
    WorkRequest wr{};
    wr.op = READ;
    wr.flag = 0;
    wr.size = sizeof(int);
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void Write_val(WorkerHandle *Cur_wh, GAddr addr, int *val)
{
    WorkRequest wr{};
    for (int i = 0; i < 1; i++)
    {
        wr.Reset();
        wr.op = WRITE;
        // wr.flag = ASYNC; // 可以在这里调
        wr.size = sizeof(int);
        wr.addr = addr;
        wr.ptr = (void *)val;
        if (Cur_wh->SendRequest(&wr))
        {
            epicLog(LOG_WARNING, "send request failed");
        }
    }
}

void ProcessWrite(WorkerHandle *Cur_wh, GAddr addr, int *val)
{
    Write_val(Cur_wh, addr, val);
}

void ProcessRead_Write(WorkerHandle *Cur_wh, GAddr addr, GAddr addr2, int *val)
{
    Read_val(Cur_wh, addr, val);
    Write_val(Cur_wh, addr2, val);
    Read_val(Cur_wh, addr2, val);
}

atomic<int> Count;

void Random_read(WorkerHandle *Cur_wh, GAddr addr, int *val)
{
    int x = 1;
    Count += x;
    while (x--)
    {
        Read_val(Cur_wh, addr, val);
    }
}

void PrivateRead(WorkerHandle *Cur_wh, GAddr addr, int *val, int num)
{
    while (num--)
    {
        int x = rand() % 2;
        if (x == 0)
            Read_val(Cur_wh, addr, val);
        else
        {
            Write_val(Cur_wh, addr, val);
            Send_Fence(Cur_wh);
        }
    }
}

void Test_Portion()
{ // 随机的读写，读写比例，节点自定义
    Count = 0;
    GAddr addr = Malloc_addr(wh[1], sizeof(int), 0, 1);
    printf("addr : %ld\n", addr);
    // Start iteration
    int Iteration = 2;
    printf("Start\n");
    long Start = get_time();
    for (int round = 0; round < Iteration; ++round)
    {
        int val = rand();
        std::list<std::thread *> threads;
        for (int i = 0; i < num_worker; ++i)
        {
            // 创建一个存储 std::thread 指针的容器
            std::thread *thread_new = new std::thread(PrivateRead, wh[i], addr, &val, i == 0 ? 180 : 1);
            threads.push_back(thread_new);

            // Read_val (wh[i], addr, &val);
            /*if (val != Lastval) {
              printf ("node : %d, val : %d, Lastval : %d\n", i+1, val, Lastval);
              flag = 1;
            }*/
        }
        for (auto thread : threads)
        {
            thread->join();
        } // 阻塞主线程等待子线程执行完毕
        for (auto thread : threads)
        {
            delete thread;
        }
    }

    printf("read portion : %.5lf\n", 1.0 * 100000 / (1.0 * Count));

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);
}

void Test_Communicate()
{
    GAddr addr = Malloc_addr(wh[0], BLOCK_SIZE, Write_shared, 2);
    printf("addr : %ld\n", addr);
    // Start iteration
    int Iteration = 2;
    printf("Start\n");
    long Start = get_time();
    for (int round = 0; round < Iteration; ++round)
    {
        int val = rand();
        Write_val(wh[0], addr, &val);
    }
    // 144069640
    // 27849288
    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);
}

void Solve()
{
    printf("111");
    Create_master();
    for (int i = 0; i < 3; ++i)
    {
        Create_worker();
    }

    sleep(1);
    // Test_Communicate();
    // Test_Portion();
    // return;

    GAddr addr, addr1, addr2, addr3;
    addr = Malloc_addr(wh[0], BLOCK_SIZE, (int)(Write_shared), 2);
    // addr = Malloc_addr(wh[0], BLOCK_SIZE, (int)(Read_mostly), 1);
    addr1 = addr + 100;
    addr2 = addr + 250;
    addr3 = addr + 350;

    int val_temp = 345;
    Write_val(wh[0], addr1, &val_temp);
    Read_val(wh[1], addr1, &val_temp);
    Read_val(wh[2], addr1, &val_temp);

    //  Start iteration
    int Iteration = 100000;
    printf("Start\n");

    long Start = get_time();
    int val1 = 1, val2 = 2, val3 = 3;
    int Lastval1, Lastval2, Lastval3;
    for (int round = 0; round < Iteration; ++round)
    {
        val1 = rand();
        val2 = rand();
        val3 = rand();
        Lastval1 = val1;
        Lastval2 = val2;
        Lastval3 = val3;

        // printf("round : %d\n", round);

        // Write_val(wh[1], addr2, &val2);
        // Write_val(wh[2], addr3, &val3);
        std::list<std::thread *> threads;
        std::thread *thread_new1 = new std::thread(Write_val, wh[1], addr2, &val2);
        std::thread *thread_new2 = new std::thread(Write_val, wh[2], addr3, &val3);
        Write_val(wh[0], addr1, &val1);

        threads.push_back(thread_new1);
        threads.push_back(thread_new2);
        // threads.push_back(thread_new3);
        for (auto thread : threads)
        {
            thread->join();
        }
        threads.clear();
        Read_val(wh[0], addr1, &val1);
        Read_val(wh[1], addr2, &val2);
        Read_val(wh[2], addr3, &val3);

        // thread_new1 = new std::thread(Read_val, wh[1], addr2, &val2);
        // thread_new2 = new std::thread(Read_val, wh[2], addr3, &val3);
        // threads.push_back(thread_new1);
        // threads.push_back(thread_new2);
        // for (auto thread : threads)
        // {
        //     thread->join();
        // }

        // epicLog(LOG_PQ, "after:val1=%d, val2=%d, val3=%d\n", val1, val2, val3);

        if (val1 != Lastval1 || val2 != Lastval2 || val3 != Lastval3)
        {
            printf("wrong on %d round\n", round);
            printf("val1 : %d, Lastval1 : %d\n", val1, Lastval1);
            printf("val2 : %d, Lastval2 : %d\n", val2, Lastval2);
            printf("val3 : %d, Lastval3 : %d\n", val3, Lastval3);
        }
        // printf("val1 : %d, Lastval1 : %d\n", val1, Lastval1);
        // printf("val2 : %d, Lastval2 : %d\n", val2, Lastval2);
        // printf("val3 : %d, Lastval3 : %d\n", val3, Lastval3);

        for (auto thread : threads)
        {
            delete thread;
        }
    }

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);
}

void ReadWrite_val(WorkerHandle *Cur_wh, GAddr addr, int *val)
{
    WorkRequest wr{};
    for (int i = 0; i < 100; i++)
    {
        wr.Reset();
        int op_id = rand() % 2;
        int addr_id = rand() % 3;
        if (op_id == 0)
        {
            wr.op = READ;
        }
        else
        {
            wr.op = WRITE;
        }
        if (addr_id == 0)
        {
            addr = addr1;
        }
        else if (addr_id == 1)
        {
            addr = addr2;
        }
        else
        {
            addr = addr3;
        }
        // wr.flag = ASYNC; // 可以在这里调
        wr.size = sizeof(int);
        wr.addr = addr;
        wr.ptr = (void *)val;
        if (Cur_wh->SendRequest(&wr))
        {
            epicLog(LOG_WARNING, "send request failed");
        }
    }
}

void Solve1()
{
    printf("111");
    Create_master();
    for (int i = 0; i < 3; ++i)
    {
        Create_worker();
    }

    sleep(1);
    // Test_Communicate();
    // Test_Portion();
    // return;

    
    // addr = Malloc_addr(wh[0], BLOCK_SIZE, (int)(Write_shared), 2);
    addr = Malloc_addr(wh[0], BLOCK_SIZE, (int)(Read_mostly), 1);
    addr1 = addr + 100;
    addr2 = addr + 250;
    addr3 = addr + 350;

    int val_temp = 345;
    Write_val(wh[0], addr1, &val_temp);
    Read_val(wh[1], addr1, &val_temp);
    Read_val(wh[2], addr1, &val_temp);

    //  Start iteration
    int Iteration = 1000;
    printf("Start\n");

    long Start = get_time();
    int val1, val2, val3;
    int Lastval1, Lastval2, Lastval3;
    for (int round = 0; round < Iteration; ++round)
    {
        val1 = rand();
        val2 = rand();
        val3 = rand();
        ReadWrite_val(wh[0], addr, &val1);

        std::list<std::thread *> threads;
        std::thread *thread_new1 = new std::thread(ReadWrite_val, wh[1], addr, &val2);
        std::thread *thread_new2 = new std::thread(ReadWrite_val, wh[2], addr, &val3);

        threads.push_back(thread_new1);
        threads.push_back(thread_new2);
        for (auto thread : threads)
        {
            thread->join();
        }

        for (auto thread : threads)
        {
            delete thread;
        }
    }

    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);
}
