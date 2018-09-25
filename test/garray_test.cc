// Copyright (c) 2018 The GAM Authors 


#include <thread>
#include "garray.h"

using std::cout;

template<typename T, size_t BUCKET_SIZE>
void Populate(GArray<T, BUCKET_SIZE>& ga, size_t size, GAlloc* allocator =
                  nullptr) {
  for (size_t i = 0; i < size; i++) {
    int w = i;
    ga.Write(i, &w);
  }
}

template<typename T, size_t BUCKET_SIZE>
void Test(GArray<T, BUCKET_SIZE>& ga, size_t size,
          GAlloc* allocator = nullptr) {
  int b;
  for (size_t i = 0; i < size; i++) {
    int w = i;
    ga.Read(i, &b);
    epicAssert(w == b);
  }
}

size_t org_size = 0;
template<typename T, size_t BUCKET_SIZE>
void TestThread(GArray<T, BUCKET_SIZE>& ga, GAlloc* galloc, bool enlarge) {
  epicLog(LOG_WARNING, "running test in worker %d", galloc->GetID());
  GArray<T, BUCKET_SIZE> gaclone(ga, galloc);
  if (enlarge) {
    epicLog(LOG_WARNING, "required to enlarge");
    gaclone.Resize(org_size * 2);
  } else {
    epicLog(LOG_WARNING, "required to shrink");
    gaclone.Resize(org_size / 2);
  }
  size_t size = org_size / 2;
  epicLog(LOG_WARNING, "test global array with size %lu", size);
  Populate(gaclone, size);
  Test(gaclone, size);
  epicLog(LOG_WARNING, "test finished");
}

int main() {
  GAlloc* allocators[4];
  ibv_device **list = ibv_get_device_list(NULL);

  Conf conf;
  conf.loglevel = LOG_WARNING;
  conf.is_master = true;
  conf.worker_port = 12346;

  GAllocFactory::SetConf(&conf);
  Master* master = new Master(conf);

  RdmaResource* res = new RdmaResource(list[0], false);
  Worker* worker = new Worker(conf, res);
  ;
  GAlloc* allocator = allocators[0] = new GAlloc(worker);
  allocators[1] = new GAlloc(worker);

  Conf conf2;
  conf2.is_master = false;
  conf2.worker_port = 12347;
  RdmaResource* res2 = new RdmaResource(list[0], false);
  Worker* worker2 = new Worker(conf2, res2);
  allocators[2] = new GAlloc(worker2);
  allocators[3] = new GAlloc(worker2);

  int b;
  size_t size = 513;

  sleep(2);

  //modulo 2 block size
  GArray<int, 32> ga5(size, allocator, RANDOM);
  epicAssert(ga5.Layers() == 5);
  epicAssert(ga5.Capacity() == 520);
  Populate(ga5, size);
  Test(ga5, size);

  //no modulo 2 block size
  GArray<int, 33> ga5_1(size, allocator, RANDOM);
  epicAssert(ga5_1.Layers() == 5);
  epicAssert(ga5_1.Capacity() == 520);
  Populate(ga5_1, size);
  Test(ga5_1, size);

  //layer1 array
  GArray<int, 513 * 4> ga1(size, allocator, RANDOM);
  epicAssert(ga1.Layers() == 1);
  epicAssert(ga1.Capacity() == size);
  Populate(ga1, size);
  Test(ga1, size);

  //clone array based on an initialized array
  //GAddr header = ga5_1.Globalize(allocator);
  GArray<int, 33> gaclone(ga5_1, allocator);
  //gaclone.Clone(header, allocator);
  epicAssert(gaclone.Equal(ga5_1));

  //resize test
  //enlarge case1: below capacity
  size_t old_size = size;
  size_t resize = 520;
  gaclone.Resize(resize);
  epicAssert(gaclone.Capacity() == ga5_1.Capacity());
  Test(gaclone, old_size);
  Populate(gaclone, resize);
  Test(gaclone, resize);
  Test(ga5_1, resize);

  //enlarge case2: no need to increase layers
  old_size = resize;
  resize = pow(4, 4) * 8;
  gaclone.Resize(resize);
  epicAssert(gaclone.Size() == resize);
  epicAssert(gaclone.Capacity() == resize);
  epicAssert(gaclone.Layers() == 5);
  Test(gaclone, old_size);
  Populate(gaclone, resize);
  Test(gaclone, resize);

  //enlarge case2: increase layers
  old_size = resize;
  resize = pow(4, 6) * 8;  //7 layers
  gaclone.Resize(resize);
  epicAssert(gaclone.Size() == resize);
  epicAssert(gaclone.Capacity() == ceil((double )resize / (33 / 4)) * 8);
  int layer = ceil(log((double) resize / (33 / 4)) / log((double) (33 / 8)))
      + 1;
  epicAssert(gaclone.Layers() == layer);
  Test(gaclone, old_size);
  Populate(gaclone, resize);
  Test(gaclone, resize);

  //shrink case1: no need to release blocks
  resize = pow(4, 6) * 8 - 1;
  gaclone.Resize(resize);
  epicAssert(gaclone.Size() == resize);
  epicAssert(gaclone.Capacity() == resize + 1);
  epicAssert(gaclone.Layers() == 7);
  Test(gaclone, resize);
  Populate(gaclone, resize);
  Test(gaclone, resize);

  //shrink case2: no need to reduce layers
  resize = pow(4, 5) * 8 + 1;
  gaclone.Resize(resize);
  epicAssert(gaclone.Size() == resize);
  epicAssert(gaclone.Capacity() == resize + 7);
  epicAssert(gaclone.Layers() == 7);
  Test(gaclone, resize);
  Populate(gaclone, resize);
  Test(gaclone, resize);

  //shrink case3: reduce layers
  resize = pow(4, 3) * 8 + 2;  //5 layers
  gaclone.Resize(resize);
  epicAssert(gaclone.Size() == resize);
  epicAssert(gaclone.Capacity() == resize + 6);
  epicAssert(gaclone.Layers() == 5);
  Test(gaclone, resize);
  Populate(gaclone, resize);
  Test(gaclone, resize);

  org_size = gaclone.Size();
  thread* threads[4];
  for (int i = 0; i < 4; i++) {
    threads[i] = new std::thread(TestThread<int, 33>, std::ref(gaclone),
                                 allocators[i], i % 2);
  }
  for (int i = 0; i < 4; i++) {
    threads[i]->join();
  }

  printf("test succeeded!\n");
}

