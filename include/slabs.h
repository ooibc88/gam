/* slabs memory allocation */
#ifndef SLABS_H
#define SLABS_H

#include <mutex>
#include <unordered_map>
#include <cstdint>
#include "log.h"
#include "settings.h"
#include "hashtable.h"
#include "lockwrapper.h"

#define ITEM_SIZE_MAX (1024*1024)

/* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define CHUNK_ALIGN_BYTES 8
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)
#define STAT_KEY_LEN 128
#define STAT_VAL_LEN 128

#define ITEM_SLABBED 4
#define ITEM_LINKED 1
#define ITEM_CAS 2

#define MEMCACHED_SLABS_ALLOCATE(arg0, arg1, arg2, arg3)
#define MEMCACHED_SLABS_ALLOCATE_ENABLED() (0)
#define MEMCACHED_SLABS_ALLOCATE_FAILED(arg0, arg1)
#define MEMCACHED_SLABS_ALLOCATE_FAILED_ENABLED() (0)
#define MEMCACHED_SLABS_FREE(arg0, arg1, arg2)
#define MEMCACHED_SLABS_FREE_ENABLED() (0)
#define MEMCACHED_SLABS_SLABCLASS_ALLOCATE(arg0)
#define MEMCACHED_SLABS_SLABCLASS_ALLOCATE_ENABLED() (0)
#define MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(arg0)
#define MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED_ENABLED() (0)

#define dbprintf(fmt, ...) _epicLog ((char*)__FILE__, (char*)__func__, __LINE__, LOG_DEBUG, fmt, ## __VA_ARGS__)
//#define ITEM_key(item) (((char*)&((item)->data)) \
//         + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))
#define ITEM_key(item) ((item)->data)

typedef struct {
  unsigned int size; /* sizes of items */
  unsigned int perslab; /* how many items per slab */

  void *slots; /* list of item ptrs */
  unsigned int sl_curr; /* total free items in list */

  unsigned int slabs; /* how many slabs were allocated for this class */

  void **slab_list; /* array of slab pointers */
  unsigned int list_size; /* size of prev array */

  unsigned int killing; /* index+1 of dying slab, or zero if none */
#ifdef FINE_SLAB_LOCK
  atomic<size_t> requested; /* The number of requested bytes */
  mutex lock_;
  inline void lock() {
    lock_.lock();
  }
  inline void unlock() {
    lock_.unlock();
  }
#else
  size_t requested; /* The number of requested bytes */
#endif
} slabclass_t;

/**
 * Structure for storing items within memcached.
 */
typedef struct _stritem {
  struct _stritem *next;
  struct _stritem *prev;
  int size;
  uint8_t it_flags; /* ITEM_* above */
  uint8_t slabs_clsid;/* which slab class we're in */
  void* data;
} item;

class SlabAllocator {
  /* powers-of-N allocation structures */
  slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
  size_t mem_limit = 0;
  size_t mem_malloced = 0;
  int power_largest;

  char *mem_base = NULL;
#ifdef FINE_SLAB_LOCK
  atomic<char *> mem_current;  // = NULL;
  atomic<size_t> mem_avail;
  atomic<size_t> mem_free;
#else
  char* mem_current = NULL;
  size_t mem_avail = 0;
  size_t mem_free;
#endif

  int SB_PREFIX_SIZE = 0;  //sizeof(item);

#ifdef FINE_SLAB_LOCK
  HashTable<void*, item*> stats_map;
#else
  unordered_map<void*, item*> stats_map;
#endif

#ifdef DHT
	/*
	 * FIXME: not support free for now
	 */
	unordered_map<void*, size_t> bigblock_map;
#endif

  //TODO: no init func
  int chunk_size = 48;
  int item_size_max = ITEM_SIZE_MAX;
  size_t maxbytes = 64 * 1024 * 1024;
  bool slab_reassign = true;

#ifndef FINE_SLAB_LOCK
  LockWrapper lock_;
#endif

  /**
   * Access to the slab allocator is protected by this lock
   */
  //static pthread_mutex_t slabs_lock = PTHREAD_MUTEX_INITIALIZER;
  //static pthread_mutex_t slabs_rebalance_lock = PTHREAD_MUTEX_INITIALIZER;
  int nz_strcmp(int nzlength, const char *nz, const char *z);
  void do_slabs_free(void *ptr, const size_t size, unsigned int id);
  void* do_slabs_alloc(const size_t size, unsigned int id);
  int do_slabs_newslab(const unsigned int id);
  void split_slab_page_into_freelist(char *ptr, const unsigned int id);
  int grow_slab_list(const unsigned int id);
  void* memory_allocate(size_t size);
  void slabs_preallocate(const unsigned int maxslabs);
  void* mmap_malloc(size_t size);
  void mmap_free(void* ptr);

  /** Allocate object of given length. 0 on error *//*@null@*/
  void *slabs_alloc(const size_t size, unsigned int id);

  /** Free previously allocated object */
  void slabs_free(void *ptr, size_t size, unsigned int id);

  /** Adjust the stats for memory requested */
  void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal);

  inline void lock() {
#ifndef FINE_SLAB_LOCK
    lock_.lock();
#endif
  }

  inline void unlock() {
#ifndef FINE_SLAB_LOCK
    lock_.unlock();
#endif
  }
  void slabs_destroy();

  /**
   * Given object size, return id to use when allocating/freeing memory for object
   * 0 means error: can't store such a large object
   */

  unsigned int slabs_clsid(const size_t size);

 public:
  /** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
   0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
   size equal to the previous slab's chunk size times this factor.
   3rd argument specifies if the slab allocator should allocate all memory
   up front (if true), or allocate memory in chunks as it is needed (if false)
   */
  void* slabs_init(const size_t limit, const double factor,
                   const bool prealloc);
  size_t get_avail();

  void *sb_calloc(size_t count, size_t size);
  void *sb_malloc(size_t size);
  void* sb_aligned_malloc(size_t size, size_t block = BLOCK_SIZE);
  void* sb_aligned_calloc(size_t count, size_t size, size_t block = BLOCK_SIZE);
  void *sb_realloc(void * ptr, size_t size);
  size_t sb_free(void * ptr);
  bool is_free(void* ptr);
  size_t get_size(void* ptr);

  ~SlabAllocator();

};

#endif
