// Copyright (c) 2018 The GAM Authors 


#include <string>
#include <iostream>
#include <cstring>
#include "gallocator.h"
#ifdef GFUNC_SUPPORT
#include "gfunc.h"
#endif
#include "util.h"

using namespace std;

int main() {
  Conf conf;
  conf.loglevel = LOG_WARNING;
  conf.is_master = true;
  conf.worker_port = 12347;

  GAlloc* allocator = GAllocFactory::CreateAllocator(&conf);
  GAddr lptr = allocator->Malloc(sizeof(int), RANDOM);
  printf("%lx is local? %s, local addr = %p\n", lptr,
         allocator->IsLocal(lptr) == true ? "true" : "false",
         allocator->GetLocal(lptr));
  int i = 2, j = 0;
  allocator->Write(lptr, &i, sizeof(int));
  allocator->Read(lptr, &j, sizeof(int));
  cout << "j = " << j << endl;

#ifdef GFUNC_SUPPORT
  double inc = 2.2;
  uint64_t incl = force_cast<uint64_t>(inc);
  allocator->Write(lptr, &i, sizeof(int), IncrDouble, incl);
  allocator->Read(lptr, &j, sizeof(int));
  cout << "j = " << j << endl;

  allocator->Write(lptr, &i, sizeof(int), IncrDouble,
                   force_cast<uint64_t>(2.0));
  allocator->Read(lptr, &j, sizeof(int));
  cout << "j = " << j << endl;
#endif

  allocator->Free(lptr);

//	Size size = 4097;
//	char buf[size];
//	char wbuf[size];
//	for(int i = 0; i < size; i++) {
//		wbuf[i] = (char)i;
//	}
//
//	GAddr rptr = 0; // allocator->AlignedMalloc(size, REMOTE);
//	Key key = 1;
//	int s = allocator->Get(key, &rptr);
//	epicAssert(s == sizeof(GAddr));
//	epicLog(LOG_WARNING, "get key %ld: %lx", key, rptr);
//
//	sleep(1);
//
//	while(allocator->Try_RLock(rptr, size));
//	allocator->Read(rptr, buf, size);
//	for(i = 0; i < size; i++) {
//		if(buf[i] != wbuf[i]) {
//			cout << "Error" << endl;
//			printf("read buf[%d:%d] = %d\n", i, (char)i, buf[i]);
//			break;
//		}
//	}
//	if(i == size) cout << "first read succeed!" << endl;
//	allocator->UnLock(rptr, size);
//
//	while(iteration--) {
//		while (allocator->Try_WLock(rptr, size));
//		memset(buf, 0, size);
//		allocator->Write(rptr, wbuf, size);
//		for (int i = 0; i < size; i++) {
//			wbuf[i]++;
//		}
//		allocator->Write(rptr, wbuf, size);
//		allocator->Read(rptr, buf, size);
//		allocator->Read(rptr, buf, size);
//		for (i = 0; i < size; i++) {
//			if (buf[i] != wbuf[i]) {
//				cout << "Error" << endl;
//				printf("read buf[%d:%d] = %d\n", i, (char) i, buf[i]);
//				break;
//			}
//		}
//		if (i == size) {
//			cout << "read succeed (iteration = " << iteration << ")" << endl;
//		} else {
//			cout << "read failed (iteration = " << iteration << ")" << endl;
//			exit(1);
//		}
//		allocator->UnLock(rptr, size);
//	}
//
//	while(allocator->Try_WLock(rptr, size));
//	memset(buf, 0, size);
//	for(i = 0; i < size; i++) {
//		j = i+1;
//		allocator->Write(GADD(rptr, i), &j , sizeof(char));
//		j++;
//		allocator->Write(GADD(rptr, i), &j , sizeof(char));
//		allocator->Read(GADD(rptr, i), &buf[i], sizeof(char));
//		if(buf[i] != (char)j) {
//			cout << "Error" << endl;
//			printf("read buf[%d:%d] = %d\n", i, (char)j, buf[i]);
//			break;
//		}
//	}
//	if(i == size) {
//		cout << "third read succeed!" << endl;
//	} else {
//		cout << "third read failed!" << endl;
//		exit(1);
//	}
//	allocator->UnLock(rptr, size);
//
//	allocator->Free(lptr);
//	allocator->Free(rptr);
//	sleep(10);
//	cout << "done" << endl;

  return 0;
}

