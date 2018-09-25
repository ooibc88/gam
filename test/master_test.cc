// Copyright (c) 2018 The GAM Authors 

#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"

int main() {
  Conf* conf = new Conf();
  GAllocFactory::SetConf(conf);
  Master* master = new Master(*conf);
  master->Join();
  return 0;
}
