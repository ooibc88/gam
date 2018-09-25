// Copyright (c) 2018 The GAM Authors 

#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"

int main() {
  ibv_device **list = ibv_get_device_list(NULL);
  //worker1
  Conf* conf = new Conf();
  GAllocFactory::SetConf(conf);
  RdmaResource* res = new RdmaResource(list[0], false);
  Worker* worker1 = new Worker(*conf, res);

  //worker2
  conf = new Conf();
  res = new RdmaResource(list[0], false);
  conf->worker_port += 1;
  Worker* worker2 = new Worker(*conf, res);

  //worker3
  conf = new Conf();
  res = new RdmaResource(list[0], false);
  conf->worker_port += 2;
  Worker* worker3 = new Worker(*conf, res);

  worker1->Join();
  worker2->Join();
  worker3->Join();
  return 0;
}
