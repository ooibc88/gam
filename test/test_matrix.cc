#include <omp.h> 
#include <cstdio> 
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <thread>
#include <pthread.h> 
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"
#include "log.h"

using namespace std;

WorkerHandle *wh[10];
ibv_device **curlist; 
Worker *worker[10];  
Master *master;
int num_worker = 0;
int num_threads = 8;
int iteration_times=1;

void Create_master()
{
    Conf *conf = new Conf();
    // conf->loglevel = LOG_PQ;
    //conf->loglevel = LOG_TEST;
    conf->loglevel = LOG_WARNING;
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

void Read_val(WorkerHandle *Cur_wh, GAddr addr, void *val, int size)
{
    WorkRequest wr{}; 
    wr.op = READ;
    wr.flag = 0;
    wr.size = size;
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void Write_val(WorkerHandle *Cur_wh, GAddr addr, void *val, int size)
{
    WorkRequest wr{};
    for (int i = 0; i < 1; i++)
    {
        wr.Reset();
        wr.op = WRITE;
        //wr.flag = ASYNC; // 可以在这里调 
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

void Init_Gam() {
    curlist = ibv_get_device_list(NULL);
    Create_master();
    for (int i = 0; i < 8; ++i)
    {
        Create_worker();
    }

    sleep(1);
}

typedef struct Error {
	float max;
	float average;
}Error; 

static void matMultCPU_serial(WorkerHandle *, GAddr , GAddr, GAddr , int , int, int , int ,int );
void genMat(GAddr , int );
Error accuracyCheck(const float* , const float* , int );

uint64 Transfer[100], Racetime[100], Requesttime[100]; 

void Answer_check(int n, GAddr c) {
    int Round = 5;
    int Curx = 3, Cury = 15;
    for (int i = 0; i < Round; ++i) {
        int x = rand() % n;
        int y = rand() % n;
        x = Curx, y = Cury + i;
        printf ("x : %d, y : %d\n", x, y);
        float val;
        Read_val(wh[0], c + (x * n + y) * sizeof(float), &val, sizeof(float)); 
        printf (" c[%d][%d] = %.3f\n", x, y, val);  
    }

    FILE* fp = fopen ("result_ws2.txt", "w");  
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            float val;
            Read_val(wh[2], c + (i * n + j) * sizeof(float), &val, sizeof(float));
            fprintf (fp, "%lf ", val);
        } fprintf (fp, "\n");
    }
    fclose(fp);   
}  

int main(int argc, char** argv)
{
    srand(time(NULL));
    // Init Gam
    Init_Gam();  
	// Init matrix
    GAddr a, b, c, d;
	int n = 256; 
	if (argc == 2) n = atoi(argv[1]);
    a = Malloc_addr(wh[0], sizeof(float) * n * n, 0, 0);
	b = Malloc_addr(wh[1], sizeof(float) * n * n, 0, 0); 
    c = Malloc_addr(wh[2], sizeof(float) * n * n, Msi, 0);  
 
	genMat(a, n);
	genMat(b, n);
 
    sleep(5);

	clock_t start, stop;
	start = clock();
    long Start = get_time();
	////// calculation code here ///////
    num_threads = num_worker;
 
    for (int i = 0; i < num_worker; ++i) {
        Transfer[i] = wh[i]->getTransferredBytes();
        Racetime[i] = wh[i]->getracetime();
        Requesttime[i] = wh[i]->getrequesttime();
    }

    thread threads[num_threads]; 
    for (int Round = 0; Round < iteration_times; ++Round) { 
        int apartx = 2; 
        int aparty = 4;
        for (int i = 0; i < apartx; ++i) {
            int Cur_Startx = i * (n / apartx);
            int Cur_Endx = Cur_Startx + (n / apartx);

            for (int j = 0; j < aparty; ++j) {
                int Cur_Starty = j * (n / aparty);
                int Cur_Endy = Cur_Starty + (n / aparty);
                threads[i * aparty + j] = thread(matMultCPU_serial, wh[i * aparty + j], a, b, c, n, Cur_Startx, Cur_Endx, Cur_Starty, Cur_Endy); 
            }
        }
        
        for (int i = 0; i < num_threads; ++i) {
            threads[i].join(); 
        }
    } 

	////// end code  ///////
	stop = clock();
    long End = get_time();
    printf ("End\n");
    printf ("running time : %lld\n", End - Start);
	printf("CPU_Serial time: %3f ms\n", ((double)stop - start) / CLOCKS_PER_SEC * 1000.0); 
    uint64 Total_transfer = 0;
    uint64 Total_racetime = 0;
    uint64 Total_requesttime = 0;
    for (int i = 0; i < num_worker; ++i) {
        Transfer[i] = wh[i]->getTransferredBytes() - Transfer[i];
        Racetime[i] = wh[i]->getracetime() - Racetime[i]; 
        Requesttime[i] = wh[i]->getrequesttime() - Requesttime[i];
        printf ("Node %d transfer : %llu, racetime : %llu\n", i, Transfer[i], Racetime[i]);
        Total_transfer += Transfer[i];
        Total_racetime += Racetime[i];
        Total_requesttime += Requesttime[i];  
    }
    printf ("Total transfer : %llu\n", Total_transfer); 
    printf ("Total racetime : %llu\n", Total_racetime);
    printf ("Total requesttime : %llu\n", Total_requesttime);
    sleep(3);
    //Answer_check (n, c);
    float tmp;
    Read_val(wh[2], c, &tmp, sizeof(float));

	return 0;
} 

int test_iterations = 1;

static void matMultCPU_serial(WorkerHandle * Cur_wh, GAddr a, GAddr b, GAddr c, int n, int Startx, int Endx, int Starty, int Endy )
{ 
    //printf ("(%d, %d) -> (%d, %d) \n", Startx, Starty, Endx, Endy);    
    for (int oo = 0; oo < test_iterations; ++oo) {    
        for (int i = Startx; i < Endx; i++)
        {
            for (int j = Starty; j < Endy; j++) 
            {
                float t = 0; 
                for (int k = 0; k < n; k++)
                {
                    float val1 = 0, val2 = 0;
                    Read_val (Cur_wh, a + (i * n + k) * sizeof(float), &val1, sizeof(float) ); 
                    Read_val (Cur_wh, b + (k * n + j) * sizeof(float), &val2, sizeof(float) );
                    t += (float)val1 * val2;
                    //printf ("%.3f * %.3f = %.3f\n", val1, val2, t);
                    Write_val(Cur_wh, c + (i * n + j) * sizeof(float), &t, sizeof(float) );  
                } 
                //Write_val(Cur_wh, c + (i * n + j) * sizeof(float), &t, sizeof(float) );
            }
        }
    }
}

int test_val = 1;

void genMat(GAddr arr, int n)
{
	int i, j;

	for (i = 0; i < n; i++)
	{
		for (j = 0; j < n; j++)
		{
            float val = (float)rand() / RAND_MAX + (float)rand() / (RAND_MAX * RAND_MAX);
            val = (float)test_val;
            Write_val(wh[0], arr + (i * n + j) * sizeof(float), &val, sizeof(float));
            test_val = test_val + 1;
		}
	}
}
 

Error accuracyCheck(const float* a, const float* b, int n)
{
	Error err;
	err.max = 0;
	err.average = 0;
	for (int i = 0; i < n; i++)
	{
		for (int j = 0; j < n; j++)
		{
			if (b[i * n + j] != 0)
			{
				//fabs求浮点数x的绝对值
				float delta = fabs((a[i * n + j] - b[i * n + j]) / b[i * n + j]);
				if (err.max < delta) err.max = delta;
				err.average += delta;
			}
		}
	}
	err.average = err.average / (n * n);
	return err;
}