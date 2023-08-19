#include <string>
#include <cstring>
#include <iostream>
#include "gallocator.h"
#include "log.h"
#include <cstring>

#define DEBUG_LEVEL LOG_WARNING
#define STEPS 204800
#define SYNC_KEY STEPS

int node_id;
int no_node = 2;
int is_master = 1;
string ip_master = get_local_ip("eth0");
string ip_worker = get_local_ip("eth0");
int port_master = 12345;
int port_worker = 12346;

int main(int argc, char *argv[])
{
  // the first argument should be the program name
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

  cout<<"is_master="<<is_master<<",master_ip="<<ip_master<<",master_port="<<port_master<<",worker_ip="<<ip_worker<<",worker_port="<<port_worker<<endl;

  GAlloc *alloc = GAllocFactory::CreateAllocator(&conf);

  sleep(1);

  // sync with all the other workers
  // check all the workers are started
  int id;
  node_id = alloc->GetID();
  alloc->Put(SYNC_KEY + node_id, &node_id, sizeof(int));
  for (int i = 1; i <= no_node; i++)
  {
    alloc->Get(SYNC_KEY + i, &id);
    epicAssert(id == i);
  }

  GAddr lptr;
  if (is_master)
  {
    lptr = alloc->AlignedMalloc(sizeof(int) * 128);
    alloc->Put(141, &lptr, sizeof(long long));
    int Cur = node_id;
    alloc->Write(lptr, &Cur, sizeof(int));
  }
  else
  {
    alloc->Get(141, &lptr);
    int Cur = node_id;
    alloc->Write(lptr + sizeof(int) * (node_id-1), &Cur, sizeof(int));
    GAddr lptr_node2 = alloc->AlignedMalloc(sizeof(int) * 128);
    printf("lptr_node2=%lx\n", lptr_node2);
  }
  alloc->Put(SYNC_KEY * 2 + node_id, &node_id, sizeof(int));
  for (int i = 1; i <= no_node; i++)
  {
    alloc->Get(SYNC_KEY * 2 + i, &id);
    epicAssert(id == i);
  }

  printf("lptr : %lx , worker_id : %ld\n", lptr, WID(lptr));
  printf("got here!!! node_id : %d , ", node_id);
  for (int i = 0; i < 2; ++i)
  {
    int Cur;
    alloc->Read(lptr + i * sizeof(int), &Cur, sizeof(int));
    printf("lptr[%d] = %d, ", i, Cur);
  }
  cout << "ip_worker : " << ip_worker << endl;

  

  return 0;
}