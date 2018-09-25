// Copyright (c) 2018 The GAM Authors 


#include <string>
#include <cstring>
#include <iostream>
#include "gallocator.h"
#include "log.h"

//
using namespace std;

int main() {
  Conf conf;
  conf.loglevel = LOG_WARNING;
  int size = 4097;
  int iteration = 10000;

  GAlloc* allocator = GAllocFactory::CreateAllocator(&conf);
  GAlloc* allocator2 = GAllocFactory::CreateAllocator();

  sleep(2);

  GAddr lptr = allocator->AlignedMalloc(sizeof(int));
  int i = 2, j = 0;

  if (!allocator->Try_WLock(lptr, sizeof(int))) {
    allocator->Write(lptr, &i, sizeof(int));
    allocator->Read(lptr, &j, sizeof(int));
    epicLog(LOG_WARNING, "j =  %d", j);
    allocator->UnLock(lptr, sizeof(int));
  }

  GAddr lptr2 = allocator2->AlignedMalloc(size);
  epicLog(LOG_WARNING, "allocate addr at %lx", lptr2);
  char buf[size], wbuf[size];
  for (int k = 0; k < size; k++) {
    wbuf[k] = k;
  }

  Key key = 1;
  GAddr rlptr2 = 0;
  allocator->Put(key, &lptr2, sizeof(GAddr));
  int s = allocator->Get(key, &rlptr2);
  epicAssert(s == sizeof(GAddr));
  epicLog(LOG_WARNING, "get key %ld: %lx", key, rlptr2);

  sleep(2);
  while (iteration--) {
    while (allocator->Try_WLock(lptr2, size))
      ;
    allocator->Write(lptr2, wbuf, size);
    allocator->Read(lptr2, buf, size);
    allocator->UnLock(lptr2, size);
    int k;
    for (k = 0; k < size; k++) {
      if (buf[k] != wbuf[k]) {
        epicLog(LOG_WARNING, "read failed buf[%d] (%d) != wbuf[%d] (%d)", k,
                buf[k], k, wbuf[k]);
        exit(-1);
      }
    }
    if (k == size) {
      cout << "read succeed (iteration = " << iteration << ")" << endl;
    } else {
      cout << "read failed (iteration = " << iteration << ")" << endl;
      exit(1);
    }
  }

  cout << "end" << endl;
  sleep(10);
  cout << "done" << endl;

  return 0;
}

