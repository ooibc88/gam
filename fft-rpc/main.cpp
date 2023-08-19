//
// Created by hrh on 1/4/23.
//
#include <cstdio>
#include <iostream>
#include <gflags/gflags.h>
#include <photon/common/utility.h>
#include <photon/io/signal.h>
#include "rpc/server.h"
#include "fft.h"
#include <unistd.h>
#include "structure.h"
#include "gallocator.h"
using namespace std;

#define DEBUG_LEVEL LOG_WARNING
#define STEPS 204800
#define SYNC_KEY STEPS

int is_master = 0;
int is_rpc_work = 0;
int is_run = 0;
int no_node = 2;
const char *ip_master = "localhost"; // get_local_ip("eth0").c_str();
const char *ip_worker = "localhost"; // get_local_ip("eth0").c_str();

int port_master = 9991;
int port_worker = 9992;
int no_thread = 1;

int port_master_master = 1234;

std::unique_ptr<McsServer> rpcservice;
void handle_null(int) {}
void handle_term(int) { rpcservice.reset(); }

int main(int argc, char *argv[])
{
  for (int i = 1; i < argc; i++)
  {
    if (strcmp(argv[i], "--ip_master") == 0)
    {
      ip_master = argv[++i];
    }
    else if (strcmp(argv[i], "--ip_worker") == 0)
    {
      ip_worker = argv[++i];
    }
    else if (strcmp(argv[i], "--port_master") == 0)
    {
      port_master = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--port_worker") == 0)
    {
      port_worker = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--is_master") == 0)
    {
      is_master = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--no_thread") == 0)
    {
      no_thread = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--rpc_worker") == 0)
    {
      is_rpc_work = atoi(argv[++i]);
    }
    else if (strcmp(argv[i], "--is_run") == 0)
    {
      is_run = atoi(argv[++i]);
    }
    else
    {
      fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
    }
  }
  // Create_Config(is_master, ip_master, ip_worker, port_master_master, port_worker, no_thread);
  // srand(time(NULL));
  // Get_curlist();
  // if (is_master)
  // {
  //   Create_master();
  //   Create_worker(1);
  //   printf("Create_master + worker succeed\n");
  // }
  // else
  // {
  //   Create_worker(0);
  //   printf("Create_worker succeed\n");
  // }

  // conf.loglevel = DEBUG_LEVEL;
  // conf.is_master = is_master;
  // conf.master_ip = ip_master;
  // conf.master_port = port_master;
  // conf.worker_ip = ip_worker;
  // conf.worker_port = port_worker;
  Create_Config(is_master, ip_master, ip_worker, port_master, port_worker);
  printf("get here 1\n");
  Create_Alloc();
  printf("get here 2\n");


  sleep(10);

  if (is_rpc_work)
  {
    photon::init();
    DEFER(photon::fini());
    photon::sync_signal(SIGPIPE, &handle_null);
    photon::sync_signal(SIGTERM, &handle_term);
    photon::sync_signal(SIGINT, &handle_term);
    rpcservice.reset(new McsServer());
    rpcservice->run(37730);
  
  }
  if (is_run)
  {
    sleep(3);
    printf("begin solve add\n");
    Solve_add_alloc();
  }
  return 0;
}