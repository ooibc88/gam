/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
//#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <unordered_map>

#include "slabs.h"
#include "settings.h"
#include "log.h"
#include "kernel.h"

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */
unsigned int SlabAllocator::slabs_clsid(const size_t size) {
  int res = POWER_SMALLEST;

  if (size == 0)
    return 0;
  while (size > slabclass[res].size) {
    if (res++ == power_largest) /* won't fit in the biggest slab */
      return 0;
  }
  return res;
}

void* SlabAllocator::mmap_malloc(size_t size) {
  static void *fixed_base = NULL;  //(void *) (0x7fc435400000);
  epicLog(LOG_INFO, "mmap_malloc size  = %ld", size);
  void* ret;
  if (size % BLOCK_SIZE) {
    size_t old_size = size;
    size = ALIGN(size, BLOCK_SIZE);
    epicLog(LOG_WARNING, "aligned the size from %lu to %lu", old_size, size);
  }
#ifdef USE_HUGEPAGE
  ret = mmap(fixed_base, size, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANON | MAP_HUGETLB, -1, 0);
#else
  ret = mmap(fixed_base, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
#endif
  if (ret == MAP_FAILED) {
    perror("map failed");
    return NULL;
  }
  uint64_t uret = (uint64_t) ret;
  if (uret % BLOCK_SIZE) {
    uret += (BLOCK_SIZE - (uret % BLOCK_SIZE));
  }
  ret = (void*) uret;

//	if(posix_memalign(&ret, BLOCK_SIZE, size)){
//		epicLog(LOG_FATAL, "allocate memory %ld failed (%d:%s)", size, errno, strerror(errno));
//	}
  return ret;
}

void SlabAllocator::mmap_free(void* ptr) {
  uint64_t uptr = (uint64_t) ptr;
  if (uptr % BLOCK_SIZE) {
    uptr -= (BLOCK_SIZE - (uptr % BLOCK_SIZE));
  }
  munmap((void*) uptr, mem_limit);
  //free(ptr);
}

size_t SlabAllocator::get_avail() {
  return mem_free;
}

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
void* SlabAllocator::slabs_init(const size_t limit, const double factor,
                                const bool prealloc) {
  epicLog(LOG_DEBUG, "limit = %ld, factor = %lf, prealloc = %d\n", limit,
          factor, prealloc);

  int i = POWER_SMALLEST - 1;
  unsigned int size = SB_PREFIX_SIZE + chunk_size;
  unsigned int pre_size = size;
  mem_limit = limit;
  mem_free = mem_limit;

  if (prealloc) {
    /* Allocate everything in a big chunk with malloc */
    //hack by zh
    mem_base = (char*) mmap_malloc(mem_limit);
    if (mem_base != NULL) {
      dbprintf("allocate succeed\n");
      mem_current = mem_base;
      mem_avail = mem_limit;
    } else {
      fprintf(stderr, "Warning: Failed to allocate requested memory in"
              " one large chunk.\nWill allocate in smaller chunks\n");
    }
  }

  memset(slabclass, 0, sizeof(slabclass));

  while (++i < POWER_LARGEST && size <= item_size_max / factor) {
    /* Make sure items are always n-byte aligned */
    if (size % CHUNK_ALIGN_BYTES)
      size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

    if ((int) (pre_size / BLOCK_SIZE) < (int) (size / BLOCK_SIZE)
        && (size % BLOCK_SIZE)) {
      slabclass[i].size = size / BLOCK_SIZE * BLOCK_SIZE;
      slabclass[i].perslab = item_size_max / slabclass[i].size;
      //epicLog(LOG_DEBUG, "aligned slab to slabclass[i].size = %d", slabclass[i].size);
      i++;
    }

    slabclass[i].size = size;
    slabclass[i].perslab = item_size_max / slabclass[i].size;
    pre_size = size;
    size *= factor;
//  epicLog(LOG_DEBUG, "slab class %3d: chunk size %9u perslab %7u\n",
//          i, slabclass[i].size, slabclass[i].perslab);
  }

  power_largest = i;
  slabclass[power_largest].size = item_size_max;
  slabclass[power_largest].perslab = 1;
  epicLog(LOG_DEBUG, "slab class %3d: chunk size %9u perslab %7u\n", i,
          slabclass[i].size, slabclass[i].perslab);

  /* for the test suite:  faking of how much we've already malloc'd */
  {
    char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
    if (t_initial_malloc) {
      mem_malloced = (size_t) atol(t_initial_malloc);
    }

  }

  if (prealloc) {
    slabs_preallocate(power_largest);
  }
  return mem_base;
}

void SlabAllocator::slabs_preallocate(const unsigned int maxslabs) {
  int i;
  unsigned int prealloc = 0;

  /* pre-allocate a 1MB slab in every size class so people don't get
   confused by non-intuitive "SERVER_ERROR out of memory"
   messages.  this is the most common question on the mailing
   list.  if you really don't want this, you can rebuild without
   these three lines.  */

  for (i = POWER_SMALLEST; i <= POWER_LARGEST; i++) {
    if (++prealloc > maxslabs)
      return;
    if (do_slabs_newslab(i) == 0) {
      fprintf(stderr, "Error while preallocating slab memory!\n"
              "If using -L or other prealloc options, max memory must be "
              "at least %d megabytes.\n",
              power_largest);
      exit(1);
    }
  }
}

void* SlabAllocator::memory_allocate(size_t size) {
  void *ret = NULL;

  if (mem_base == NULL) {
    /* We are not using a preallocated large memory chunk */
    epicLog(LOG_FATAL, "allocator is not initialized!");
  } else {
    ret = mem_current;

    if (size > mem_avail) {
      return NULL;
    }

    /* mem_current pointer _must_ be aligned!!! */
    if (size % CHUNK_ALIGN_BYTES) {
      size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
    }

    mem_current += size;
    if (size < mem_avail) {
      mem_avail -= size;
    } else {
      mem_avail = 0;
    }
  }
  return ret;
}

int SlabAllocator::grow_slab_list(const unsigned int id) {
  slabclass_t *p = &slabclass[id];
  if (p->slabs == p->list_size) {
    size_t new_size = (p->list_size != 0) ? p->list_size * 2 : 16;
    void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
    if (new_list == 0)
      return 0;
    p->list_size = new_size;
    p->slab_list = (void **) new_list;
  }
  return 1;
}

void SlabAllocator::split_slab_page_into_freelist(char *ptr,
                                                  const unsigned int id) {
  slabclass_t *p = &slabclass[id];
  int x;
  for (x = 0; x < p->perslab; x++) {
    do_slabs_free(ptr, 0, id);
    ptr += p->size;
  }
}

int SlabAllocator::do_slabs_newslab(const unsigned int id) {
  slabclass_t *p = &slabclass[id];
  int len = slab_reassign ? item_size_max : p->size * p->perslab;
  char *ptr;

  if ((mem_limit && mem_malloced + len > mem_limit && p->slabs > 0)
      || (grow_slab_list(id) == 0)
      || ((ptr = (char *) memory_allocate((size_t) len)) == 0)) {

    epicLog(LOG_WARNING, "new slab class %d failed", id);
    return 0;
  }

  //FIXME (zh): check whether this is necessary
  //memset(ptr, 0, (size_t)len); //sep
  split_slab_page_into_freelist(ptr, id);

  p->slab_list[p->slabs++] = ptr;
  mem_malloced += len;
  MEMCACHED_SLABS_SLABCLASS_ALLOCATE(id);

  return 1;
}

/*@null@*/
void * SlabAllocator::do_slabs_alloc(const size_t size, unsigned int id) {
  slabclass_t *p;
  void *ret = NULL;
  item *it = NULL;

  if (id < POWER_SMALLEST || id > power_largest) {
    MEMCACHED_SLABS_ALLOCATE_FAILED(size, 0);
    return NULL;
  }

  p = &slabclass[id];
#ifdef FINE_SLAB_LOCK
  p->lock();
#endif

  //printf("sl_curr = %d, ((item *)p->slots)->slabs_clsid = %d\n", p->sl_curr, ((item *)p->slots)->slabs_clsid);
  assert(p->sl_curr == 0 || ((item * )p->slots)->slabs_clsid == 0);

  /* fail unless we have space at the end of a recently allocated page,
   we have something on our freelist, or we could allocate a new page */
  if (!(p->sl_curr != 0 || do_slabs_newslab(id) != 0)) {
    /* We don't have more memory available */
    ret = NULL;
  } else if (p->sl_curr != 0) {
    /* return off our freelist */
    it = (item *) p->slots;
    p->slots = it->next;
    if (it->next)
      it->next->prev = 0;
    //hack by zh
    it->size = size;
    it->slabs_clsid = id;

    p->sl_curr--;
    //ret = (void *)it; //sep
    ret = it->data;  //sep
  }
#ifdef FINE_SLAB_LOCK
  p->unlock();
#endif

  if (ret) {
    p->requested += size;
    MEMCACHED_SLABS_ALLOCATE(size, id, p->size, ret);
    mem_free -= p->size;
  } else {
    MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
  }

  return ret;
}

void SlabAllocator::do_slabs_free(void *ptr, const size_t size,
                                  unsigned int id) {
  slabclass_t *p;
  item *it;

  //assert(((item *)ptr)->slabs_clsid == 0); //sep
  assert(id >= POWER_SMALLEST && id <= power_largest);
  if (id < POWER_SMALLEST || id > power_largest)
    return;

  MEMCACHED_SLABS_FREE(size, id, ptr);
  p = &slabclass[id];

  //it = (item *)ptr; //sep
  if (stats_map.count(ptr)) {
    it = stats_map.at(ptr);  //sep
  } else {
    it = new item();
    stats_map[ptr] = it;
  }
  it->data = ptr;  //sep

  it->it_flags |= ITEM_SLABBED;
  it->prev = 0;
  it->next = (struct _stritem *) p->slots;
  if (it->next)
    it->next->prev = it;
  p->slots = it;

  p->sl_curr++;
  p->requested -= size;
  if (size)
    mem_free += p->size;
  return;
}

/*
 * return the ptr where data should be stored without the header
 */
void * SlabAllocator::sb_malloc(size_t size) {

#ifdef DHT
	/*
	 * enable to allocate memory larger than 1M
	 * FIXME: not support free of large block for now
	 */
	if(size > item_size_max) {
		lock();
		void* ret = memory_allocate(size);
		epicLog(LOG_WARNING, "allocate memory %lu, larger than default max %d, at %lx", size, item_size_max, ret);
		epicAssert(((uint64_t)ret % BLOCK_SIZE) == 0);
		bigblock_map[ret] = size;
		unlock();
		return ret;
	}
#endif

  lock();
  /*
   * if the slab-allocator isn't initiated, we use the default malloc()!
   */
  if (mem_limit == 0) {
    dbprintf("sb_mallocator is not initiated. Use default malloc\n");
    return NULL;
  }

  size_t newsize = size + SB_PREFIX_SIZE;

  unsigned int id = slabs_clsid(newsize);
  //item * ret = (item *)slabs_alloc(newsize, id); //sep
  //return ret == NULL ? NULL : ITEM_key(ret); //sep
  void* ret = slabs_alloc(newsize, id);  //sep
  epicAssert(ret);
  unlock();
  return ret;
}

void * SlabAllocator::sb_aligned_malloc(size_t size, size_t block) {

#ifdef DHT
	/*
	 * enable to allocate memory larger than 1M
	 * FIXME: not support free of large block for now
	 */
	if(size > item_size_max) {
		epicLog(LOG_WARNING, "allocate memory %lu, larger than default max %lu", size, item_size_max);
		lock();
		void* ret = memory_allocate(size);
		epicAssert((uint64_t)ret % BLOCK_SIZE == 0);
		bigblock_map[ret] = size;
		unlock();
		return ret;
	}
#endif

  lock();
  /*
   * if the slab-allocator isn't initiated, we use the default malloc()!
   */
  if (mem_limit == 0) {
    dbprintf("sb_mallocator is not initiated. Use default malloc\n");
    return NULL;
  }

  size_t newsize = size + SB_PREFIX_SIZE;

  newsize = ALIGN(newsize, block);

  unsigned int id = slabs_clsid(newsize);
  //item * ret = (item *)slabs_alloc(newsize, id); //sep
  //return ret == NULL ? NULL : ITEM_key(ret); //sep
  void* ret = slabs_alloc(newsize, id);  //sep
  epicAssert(ret);
  epicLog(LOG_DEBUG, "ret = %lx, newsize = %d", ret, newsize);
  epicAssert((uint64_t )ret % block == 0);
  unlock();
  return ret;
}

/*
 * return the ptr where data should be stored without the header
 */
void * SlabAllocator::sb_calloc(size_t count, size_t size) {

  dbprintf("sb_calloc size = %ld\n", size);

  if (unlikely(mem_limit == 0)) {
    dbprintf("using default calloc\n");
    return NULL;
  }

  void * ptr = sb_malloc(count * size);
  if (ptr != NULL) {
    epicLog(LOG_INFO,
            "WARNING: touch the registered memory area during allocation!!!");
    memset(ptr, 0, size);
  } else {
      epicAssert(false);
  }
  return ptr;
}

void * SlabAllocator::sb_aligned_calloc(size_t count, size_t size,
                                        size_t block) {

  dbprintf("sb_calloc size = %ld\n", size);

  if (unlikely(mem_limit == 0)) {
    dbprintf("using default calloc\n");
    return NULL;
  }

  void * ptr = sb_aligned_malloc(count * size, block);
  if (ptr != NULL) {
    epicLog(LOG_INFO,
            "WARNING: touch the registered memory area during allocation!!!");
    memset(ptr, 0, size);
  } else {
    epicLog(LOG_WARNING, "no free memory");
    epicAssert(false);
  }
  return ptr;
}

/*
 * return the ptr where data should be stored without the header
 */
void *SlabAllocator::sb_realloc(void * ptr, size_t size) {

  dbprintf("sb_realloc size = %ld\n", size);
  /*
   * if the slab-allocator isn't initiated, we use the default realloc()!
   */
  if (unlikely(mem_limit == 0)) {
    dbprintf("using default realloc\n");
    return NULL;
  }

  if (ptr == NULL)
    return sb_malloc(size);

  lock();
  //item * it1 = (item *) ((char*)ptr-SB_PREFIX_SIZE); //sep
  epicAssert(stats_map.count(ptr));  //sep
  item* it1 = stats_map.at(ptr);  //sep
  unsigned int id1 = it1->slabs_clsid;
  int size1 = it1->size;
  epicAssert(id1 == slabs_clsid(size1));

  size_t size2 = size + SB_PREFIX_SIZE;
  unsigned int id2 = slabs_clsid(size2);
  void* ret = nullptr;
  if (id1 == id2) {
    it1->size = size2;
    slabs_adjust_mem_requested(id1, size1, size2);
    ret = ptr;
  } else {
    epicAssert(size1 != size2);
    //item * it2 = (item *)slabs_alloc(size2, id2); //sep
    void* ptr = slabs_alloc(size2, id2);  //sep
    epicAssert(stats_map.count(ptr));  //sep
    item* it2 = stats_map.at(ptr);  //sep

    if (size2 < size1)
      memcpy(ITEM_key(it2), ptr, size);
    else
      memcpy(ITEM_key(it2), ptr, size1 - SB_PREFIX_SIZE);

    //need to clear the clsid as the original memcached implementation check this in free function
    it1->slabs_clsid = 0;
    //slabs_free(it1, size1, id1); //sep
    slabs_free(it1->data, size1, id1);  //sep
    ret = ITEM_key(it2);
  }
  unlock();
  epicAssert(ret);
  return ret;
}

bool SlabAllocator::is_free(void* ptr) {
  lock();
  epicAssert(stats_map.count(ptr));  //sep
  item* it = stats_map.at(ptr);  //sep
  bool ret = it->slabs_clsid == 0 ? true : false;
  unlock();
  return ret;
}

size_t SlabAllocator::get_size(void* ptr) {
  epicAssert(stats_map.count(ptr));
  item* it = stats_map[ptr];
  return it->size;
}

size_t SlabAllocator::sb_free(void *ptr) {
  lock();

#ifdef DHT
	if(bigblock_map.count(ptr)) {
		epicLog(LOG_WARNING, "not support free of big block for now");
		unlock();
		return 0;
	}
#endif

  /*
   * if the slab-allocator isn't initiated, we use the default free()!
   */
  if (mem_limit == 0) {
    epicLog(LOG_DEBUG, "allocator is not initialized");
    return 0;
  }

  //item * it = (item *) ((char*)ptr-SB_PREFIX_SIZE); //sep
  epicAssert(stats_map.count(ptr));  //sep
  item* it = stats_map.at(ptr);  //sep
  unsigned int id = it->slabs_clsid;
  size_t size = it->size;

  assert(id == slabs_clsid(it->size));
  it->slabs_clsid = 0;
  it->size = 0;
  //slabs_free(it, it->size, id); //sep
  //FIXME: remove below
  memset(it->data, 0, size);
  slabs_free(it->data, size, id);
  unlock();
  return size;
}

void *SlabAllocator::slabs_alloc(size_t size, unsigned int id) {
  void *ret;

  ////pthread_mutex_lock(&slabs_lock);
  ret = do_slabs_alloc(size, id);
  ////pthread_mutex_unlock(&slabs_lock);
  return ret;
}

void SlabAllocator::slabs_free(void *ptr, size_t size, unsigned int id) {
  ////pthread_mutex_lock(&slabs_lock);
  slabclass_t* p = &slabclass[id];
#ifdef FINE_SLAB_LOCK
  p->lock();
#endif
  do_slabs_free(ptr, size, id);
#ifdef FINE_SLAB_LOCK
  p->unlock();
#endif
  ////pthread_mutex_unlock(&slabs_lock);
}

int SlabAllocator::nz_strcmp(int nzlength, const char *nz, const char *z) {
  int zlength = strlen(z);
  return (zlength == nzlength) && (strncmp(nz, z, zlength) == 0) ? 0 : -1;
}

void SlabAllocator::slabs_adjust_mem_requested(unsigned int id, size_t old,
                                               size_t ntotal) {
  ////pthread_mutex_lock(&slabs_lock);
  slabclass_t *p;
  if (id < POWER_SMALLEST || id > power_largest) {
    fprintf(stderr, "Internal error! Invalid slab class\n");
    abort();
  }

  p = &slabclass[id];
  p->requested = p->requested - old + ntotal;
  ////pthread_mutex_unlock(&slabs_lock);
}

SlabAllocator::~SlabAllocator() {
  if (mem_base)
    mmap_free(mem_base);
}
