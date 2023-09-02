// Copyright (c) 2018 The GAM Authors

#ifndef INCLUDE_WORKREQUEST_H_
#define INCLUDE_WORKREQUEST_H_

#include <type_traits>
#include <cstring>
#include <atomic>
#include <mutex>
#include <unistd.h>
#include <syscall.h>

#include "lockwrapper.h"
#include "structure.h"
#include "zmalloc.h"
#ifdef GFUNC_SUPPORT
#include "gfunc.h"
#endif

enum Work
{
  MALLOC = 1,
  READ,
  FETCH_AND_SHARED,
  READ_FORWARD,
  WRITE,
  WRITE_PERMISSION_ONLY,
  FETCH_AND_INVALIDATE,
  INVALIDATE,
  INVALIDATE_FORWARD,
  WRITE_FORWARD,
  WRITE_PERMISSION_ONLY_FORWARD,
  ATOMIC,
  UPDATE_MEM_STATS,
  FETCH_MEM_STATS,
  BROADCAST_MEM_STATS,
  MFENCE,
  SFENCE,
  RLOCK,
  RLOCK_LEN,
  WLOCK,
  WLOCK_LEN,
  UNLOCK,
  UNLOCK_LEN,
  FREE,
  ACTIVE_INVALIDATE,
  WRITE_BACK,
  PENDING_INVALIDATE,
  PUT,
  GET,
  /* add ergeda add */
  JUST_WRITE, // access_exclusive write
  SET_CACHE,  // create cache when malloc
  JUST_READ,  // access_exclusive read
  READ_TYPE,
  RM_WRITE,   // read_mostly write
  RM_READ,    // read_mostly read
  RM_FORWARD, // read_mostly forward_write
  RM_Done,    // read_mostly forward_write_done
  TEST_RDMA,  // used to learn rdma workflow
  WE_READ,    // write exclusive read
  WE_WRITE,   // write exclusive write(!owner_node)
  WE_INV,     // write_exclusive invalidate
#ifdef DYNAMIC
  CHANGE,
#endif

#ifdef B_I
  BI_READ,
  BI_WRITE,
  BI_INV,
  BI_INFORM, // 通知版本
#endif
/* add ergeda add */
#ifdef DHT
  GET_HTABLE,
#endif
  // set the value of REPLY so that we can test op & REPLY
  // to check whether it is a reply workrequest or not
  REPLY = 1 << 16,
#ifdef NOCACHE
  RLOCK_REPLY,
  WLOCK_REPLY,
#ifndef ASYNC_UNLOCK
  UNLOCK_REPLY,
#endif
#endif
  MALLOC_REPLY,
  FETCH_MEM_STATS_REPLY,
  READ_REPLY,
  WRITE_REPLY,
  LOCK_REPLY,
  FREE_REPLY,
#ifdef DHT
  GET_HTABLE_REPLY,
#endif
  GET_REPLY,
  /* add ergeda add */
  TYPE_REPLY,
  JUST_READ_REPLY,
  SET_CACHE_REPLY,
  /* add ergeda add */

  /* add wpq add*/
  writeshared_WRITE,
  writeshared_WRITE_REPLY,
  ChangeSubLog,
  writeshared_READ,
  completeFlush,
  flushToHomeOp,
  InitAcquire
  /* add wpq add */
};

enum Status
{
  SUCCESS = 0,
  REMOTE_REQUEST,
  IN_TRANSITION,
  FENCE_PENDING,
  READ_SUCCESS,
  ERROR = 1 << 8,
  ALLOC_ERROR,
  READ_ERROR,
  WRITE_ERROR,
  UNRECOGNIZED_OP,
  LOCK_FAILED,
  NOT_EXIST
};

typedef std::underlying_type<Work>::type wtype;
typedef std::underlying_type<Status>::type stype;

typedef int Flag;

#define REMOTE 1
#define RANDOM (1 << 1)
#define CACHED (1 << 2)
#define ASYNC (1 << 3)
#define REPEATED (1 << 4)
#define REQUEST_DONE (1 << 5)
#define LOCKED (1 << 6)
#define TRY_LOCK (1 << 7)
#define TO_SERVE (1 << 8)
#define ALIGNED (1 << 9)
#define COPY (1 << 10)
#define LOCAL_REQUEST (1 << 11)
#define FENCE (1 << 12)
#define NOT_CACHE (1 << 13)
#define GFUNC (1 << 14)
/* add ergeda add */
#define Msi (1 << 15)
#define Read_only (1 << 16)
#define Read_mostly (1 << 17)
#define Access_exclusive (1 << 18)
#define Write_exclusive (1 << 19)
#define Write_shared (1 << 20)
#define RC_Write_shared (1 << 21)
#define Bounded_incoherence (1 << 22)

#define Add_list (1 << 23) // 表示是第一次访问，需要加入shared_list(read_mostly)
#ifdef DYNAMIC
#define CheckChange (1 << 24) //检查当前指令是否为目录转换专用
#endif

#ifdef B_I
#define b_i (1 << 25)
#endif
/* add ergeda add */

#define MASK_ID 1
#define MASK_OP 1 << 1
#define MASK_ADDR 1 << 2
#define MASK_FREE 1 << 3
#define MASK_SIZE 1 << 4
#define MASK_STATUS 1 << 5
#define MASK_FLAG 1 << 6
#define MASK_PTR 1 << 7
#define MASK_FD 1 << 8
#define MASK_WID 1 << 9
#define MASK_COUNTER 1 << 10

/*
 * TODO: try to shrink the size of WorkRequest structure
 * use union?
 */
struct WorkRequest
{
  unsigned int id;  // identifier of the work request
  unsigned int pid; // identifier of the parent work request (used for FORWARD request)
  atomic<int> flush_id;
  int pwid; // identifier of the parent worker
  enum Work op;

  union
  {
    uint64_t key;
    GAddr addr;
    Size free;
  };
  Size size;
  int status;

  Flag flag = 0;
  int flagSub1;
  int flagSub2;

  void *ptr;
  uint64_t arg = 0;

  int fd;
#if !defined(USE_PIPE_W_TO_H) || !defined(USE_PIPE_H_TO_W)
  volatile int *notify_buf;
#endif
#ifdef USE_PTHREAD_COND
  pthread_mutex_t *cond_lock;
  pthread_cond_t *cond;
#endif

  int wid;

  atomic<int> counter; // maybe negative in Write Case 4

  WorkRequest *parent;
  WorkRequest *next; //
  WorkRequest *dup;

  LockWrapper lock_;

  bool is_cache_hit_ = true;
#ifdef DYNAMIC
  int Version = 1;
#endif

#ifdef GFUNC_SUPPORT
  GFunc *gfunc = nullptr;
  
#endif
  WorkRequest()
      : fd(),
        id(),
        pid(),
        pwid(),
        op(),
        addr(),
        size(),
        status(),
        flag(),
        ptr(),
        wid(),
        counter(),
        parent(),
        next(),
        dup()
  {
#if !defined(USE_PIPE_W_TO_H) || !defined(USE_PIPE_H_TO_W)
    notify_buf = nullptr;
#endif
  };
  WorkRequest(WorkRequest &wr);
  bool operator==(const WorkRequest &wr);
  int Ser(char *buf, int &len);
  int Deser(const char *buf, int &len);

  // we only allow one-times copy of the original workrequest
  // second call will return the previous duplicated copy
  // NOTE: if you want multiple copies,
  // use the WorkRequest(WorkRequest&) constructor
  WorkRequest *Copy()
  {
    if (flag & COPY)
    {
      epicLog(LOG_DEBUG, "already copied before");
      if (dup)
      {
        return dup;
      }
      else
      {
        return this; // this is a copied version
      }
    }
    else
    {
      WorkRequest *nw = new WorkRequest(*this);
      if (ptr && size)
      {
        nw->ptr = zmalloc(size);
        memcpy(nw->ptr, ptr, size);
      }
      nw->flag |= COPY;
      // update the original version
      flag |= COPY;
      dup = nw;
      return nw;
    }
  }

  bool IsACopy()
  {
    return (flag & COPY) && (dup == nullptr);
  }

  void Reset()
  {
    lock();
    // memset(this, 0, sizeof(WorkRequest));
    id = 0; // identifier of the work request

    pid = 0;  // identifier of the parent work request (used for FORWARD request)
    pwid = 0; // identifier of the parent worker
    op = static_cast<Work>(0);

    key = 0;
    addr = 0;
    free = 0;
    size = 0;
    status = 0;

    flag = 0;
    ptr = 0;
    fd = 0;
#if !defined(USE_PIPE_W_TO_H) || !defined(USE_PIPE_H_TO_W)
    notify_buf = 0;
#endif
    wid = 0;
    counter.store(0);
    parent = nullptr;
    next = nullptr;
    dup = nullptr;
#ifdef GFUNC_SUPPORT
    gfunc = nullptr;
    arg = 0;
#endif
#ifdef DYNAMIC
    Version = 0;
#endif

    is_cache_hit_ = true;
    unlock();
  }

  inline void lock()
  {
    lock_.lock();
  }

  inline void unlock()
  {
    lock_.unlock();
  }

  ~WorkRequest();
};

#endif /* INCLUDE_WORKREQUEST_H_ */
