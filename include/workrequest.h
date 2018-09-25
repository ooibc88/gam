// Copyright (c) 2018 The GAM Authors 

#ifndef INCLUDE_WORKREQUEST_H_
#define INCLUDE_WORKREQUEST_H_

#include <type_traits>
#include "structure.h"

enum Work {
  FETCH_MEM_STATS = 1,
  UPDATE_MEM_STATS,
  BROADCAST_MEM_STATS,
  PUT,
  GET,
  KV_PUT,
  KV_GET,
  FARM_MALLOC,
  FARM_READ,
  PREPARE,
  VALIDATE,
  COMMIT,
  ABORT,
  //set the value of REPLY so that we can test op & REPLY
  //to check whether it is a reply workrequest or not
  REPLY = 1 << 16,
  FARM_MALLOC_REPLY,
  FARM_READ_REPLY,
  VALIDATE_REPLY,
  PREPARE_REPLY,
  ACKNOWLEDGE,
  FETCH_MEM_STATS_REPLY,
  GET_REPLY,
  PUT_REPLY,
};

enum Status {
  SUCCESS = 0,
  ALLOC_ERROR,
  READ_SUCCESS,
  READ_ERROR,
  WRITE_ERROR,
  UNRECOGNIZED_OP,
  LOCK_FAILED,
  PREPARE_FAILED,
  VALIDATE_FAILED,
  COMMIT_FAILED,
  NOT_EXIST
};


class TxnContext;
using wtype = std::underlying_type<Work>::type;
using stype = std::underlying_type<Status>::type;
const char* workToStr(Work);

using Flag = int;

#define REMOTE 1
#define SPREAD 1 << 1
#define CACHED 1 << 2
#define ASYNC 1 << 3
#define REPEATED 1 << 4
#define REQUEST_DONE 1 << 5
#define LOCKED 1 << 6
#define TRY_LOCK 1 << 7
#define TO_SERVE 1 << 8
#define ALIGNED 1 << 9

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
struct WorkRequest {
  uint32_t id;
  union {
    uint32_t nobj;
    TxnContext* tx;
  };

  unsigned int pid; //identifier of the parent work request (used for FORWARD request)
  int pwid; //identifier of the parent worker
  enum Work op;

  union {
    uint64_t key;
    GAddr addr;
    Size free;
  };
  Size size;
  int status;

  Flag flag;
  void* ptr;

  int fd;
#if	!(defined(USE_PIPE_W_TO_H) && defined(USE_PIPE_H_TO_W))
  volatile int* notify_buf;
#endif
#ifdef USE_PTHREAD_COND
  pthread_mutex_t* cond_lock;
  pthread_cond_t* cond;
#endif

  int wid;

  int counter; //maybe negative in Write Case 4

  WorkRequest* parent;
  WorkRequest* next;

  WorkRequest(): fd(), id(-1), pid(), pwid(), op(), addr(), size(), status(),
  flag(), ptr(), wid(), counter(), parent(), next() {
#if !(defined(USE_PIPE_H_TO_W) && defined(USE_PIPE_W_TO_H))
    notify_buf = nullptr;
#endif
  };
  WorkRequest(WorkRequest& wr);
  bool operator==(const WorkRequest& wr);
  int Ser(char* buf, int& len);
  int Deser(const char* buf, int& len);

  ~WorkRequest();
};


#endif /* INCLUDE_WORKREQUEST_H_ */
