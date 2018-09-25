// Copyright (c) 2018 The GAM Authors 


#include <cstring>
#include <sys/mman.h>
#include <iostream>
#include "slabs.h"
#include "gallocator.h"
#include "structure.h"
#include <iostream>
using namespace std;

int main() {
  const Conf* conf = GAllocFactory::InitConf();
  SlabAllocator sb { };
  void* base = sb.slabs_init(conf->size, conf->factor, true);
  int max = 1024 * 1024;
  int step = 5;
  char* buf = (char*) sb.sb_aligned_malloc(1);
  epicAssert((uint64_t)buf % BLOCK_SIZE == 0);

  size_t avail = sb.get_avail();
  cout << "avail memory: " << sb.get_avail() << endl;

  /*
   * check the functionality of sb_allocator
   */
  int i;
  for (i = 1; i < max; i += step) {
    if (sb.get_avail() != avail) {
      printf("error %dth time avail = %ld\n", i, sb.get_avail());
      break;
    }
    void* buf = sb.sb_malloc(i);
    if (!buf) {
      cout << "malloc error for size = " << i << endl;
      break;
    }
    //memset(buf, 1, i);
    epicAssert(!sb.is_free(buf));
    epicAssert(sb.get_size(buf) == i);
    sb.sb_free(buf);
    epicAssert(sb.is_free(buf));
  }

  if (i == max) {
    cout << "First test: succeed!" << endl;
  } else {
    cout << "First test: failed" << endl;
  }

  /*
   * check that the behavior of sb_allocator will not touch the memory to be allocated!
   * Otherwise, it will generate segmentation fault
   */
  mprotect(base, conf->size, PROT_NONE);
  for (i = 1; i < max; i += step) {
    if (sb.get_avail() != avail) {
      cout << i << endl;
      cout << sb.get_avail() << endl;
      break;
    }
    void* buf = sb.sb_malloc(i);
    if (!buf) {
      cout << "malloc error for size = " << i << endl;
      break;
    }
    sb.sb_free(buf);
  }
  if (i == max) {
    cout << "Second test: succeed!" << endl;
  } else {
    cout << "Second test: failed" << endl;
  }
  return 0;
}

