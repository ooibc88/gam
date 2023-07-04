// Copyright (c) 2018 The GAM Authors

#include <cstring>
#include "workrequest.h"

#include "chars.h"
#include "log.h"
#ifdef GFUNC_SUPPORT
#include "gfunc.h"
#endif

int WorkRequest::Ser(char *buf, int &len)
{
  len = 0;
  wtype lop = static_cast<wtype>(op);
  stype lstatus = static_cast<stype>(status);
  switch (op)
  {
#ifdef DHT
  case GET_HTABLE:
    len = appendInteger(buf, lop, id);
    break;
  case GET_HTABLE_REPLY:
    len = appendInteger(buf, lop, id, addr, lstatus);
    break;
#endif
  case UPDATE_MEM_STATS:
    len = appendInteger(buf, lop, wid, size, free);
    break;
  case FETCH_MEM_STATS_REPLY:
  case BROADCAST_MEM_STATS:
    len = appendInteger(buf, lop, size);
    memcpy(buf + len, ptr, strlen((char *)ptr));
    len += strlen((char *)ptr);
    break;
  case GET:
    len = appendInteger(buf, lop, id, wid, key);
    break;
  /* add ergeda add */
  case SET_CACHE:
    len = appendInteger(buf, lop, id, wid, addr, size, flag, arg);
    break;
  case SET_CACHE_REPLY:
    len = appendInteger(buf, lop, id, wid);
    break;
  case RM_READ:
  case JUST_READ:
  case WE_READ:
    len = appendInteger(buf, lop, id, wid, addr, size, ptr);
    break;
  case READ_TYPE:
    len = appendInteger(buf, lop, id, wid, addr);
    break;
  case JUST_WRITE:
  case TYPE_REPLY:
  case WE_WRITE:
    len = appendInteger(buf, lop, id, wid, addr, size, flag);
    memcpy(buf + len, ptr, size);
    len += size;
    break;
  case RM_WRITE:
    len = appendInteger(buf, lop, id, wid, addr, size, flag, next);
    memcpy(buf + len, ptr, size);
    len += size;
    break;
  case RM_FORWARD:
  case WE_INV:
    len = appendInteger(buf, lop, id, wid, addr);
    break;
  case TEST_RDMA:
    len = appendInteger(buf, lop, id, wid, addr, ptr);
    break;

  /* add ergeda add */
  case PUT:
  case GET_REPLY:
    len = appendInteger(buf, lop, id, wid, key, size);
    memcpy(buf + len, ptr, size);
    len += size;
    break;
  case FETCH_MEM_STATS:
    len = appendInteger(buf, lop, wid);
    break;

  case MALLOC:
    // len = appendInteger(buf, lop, id, wid, size, flag);
    /* add ergeda add */
    len = appendInteger(buf, lop, id, wid, size, flag, arg);
    /* add ergeda add */
    break;
  case MALLOC_REPLY:
    len = appendInteger(buf, lop, id, wid, addr, status);
    break;
  case FREE:
    len = appendInteger(buf, lop, id, wid, addr);
    break;
#ifdef NOCACHE
  case WRITE:
  case READ_REPLY:
    len = appendInteger(buf, lop, id, wid, addr, size, flag);
    memcpy(buf + len, ptr, size);
    len += size;
    break;
  case WLOCK:
  case RLOCK:
  case UNLOCK:
#else // else NOCACHE
  case WRITE:
#endif
#ifdef GFUNC_SUPPORT
  {
    int gid = GetGFuncID(gfunc);
    len = appendInteger(buf, lop, id, wid, addr, size, ptr, flag, gid, arg);
    if (flag & GFUNC)
      epicAssert(gid != -1);
  }
#ifdef SELECTIVE_CACHING
    if ((flag & NOT_CACHE) && !(flag & GFUNC))
    {
      memcpy(buf + len, ptr, size);
      len += size;
    }
#endif

#else
    len = appendInteger(buf, lop, id, wid, addr, size, ptr, flag);
#ifdef SELECTIVE_CACHING
    if (flag & NOT_CACHE)
    {
      memcpy(buf + len, ptr, size);
      len += size;
    }
#endif
#endif
    break;
  case WRITE_PERMISSION_ONLY:
#ifdef GFUNC_SUPPORT
  {
    int gid = GetGFuncID(gfunc);
    len = appendInteger(buf, lop, id, wid, addr, size, ptr, flag, gid, arg);
    if (flag & GFUNC)
      epicAssert(gid != -1);
  }
#else
    len = appendInteger(buf, lop, id, wid, addr, size, ptr, flag);
#endif
  break;
  case READ:
  case FETCH_AND_SHARED:
  case FETCH_AND_INVALIDATE:
  case INVALIDATE:
  case InitAcquire:
    len = appendInteger(buf, lop, id, wid, addr, size, ptr, flag);
    break;
  case READ_FORWARD:
  case WRITE_FORWARD:
  case INVALIDATE_FORWARD:
  case WRITE_PERMISSION_ONLY_FORWARD:
    len = appendInteger(buf, lop, id, wid, addr, size, ptr, flag, pid, pwid);
    break;
#ifndef NOCACHE
  case READ_REPLY:
#else
  case RLOCK_REPLY:
  case WLOCK_REPLY:
#ifndef ASYNC_UNLOCK
  case UNLOCK_REPLY:
#endif
#endif
    len = appendInteger(buf, lop, id, wid, status);
    break;
  case WRITE_REPLY:
    len = appendInteger(buf, lop, id, wid, status, counter.load());
    break;
  case ACTIVE_INVALIDATE:
  case WRITE_BACK:
    len = appendInteger(buf, lop, id, wid, addr, ptr);
    break;

    /* add wpq add */
  case ChangeSubLog:
    len = appendInteger(buf, lop, addr, id, wid, pid, pwid, flagSub1, flagSub2);
    break;

  case writeshared_READ:
    len = appendInteger(buf, lop, id, wid, addr, size, ptr, flag, pid, pwid);
    /* add wpq add */

  default:
    epicLog(LOG_WARNING, "unrecognized op code");
    break;
  }
  buf[len] = '\0';
  epicLog(LOG_DEBUG, "ser %s", buf);
  return 0;
}

int WorkRequest::Deser(const char *buf, int &len)
{
  int ret;
  len = 0;
  char *p = (char *)buf;
  wtype lop;
  p += readInteger(p, lop);
  op = static_cast<Work>(lop);
  stype s;
  switch (op)
  {
#ifdef DHT
  case GET_HTABLE:
    p += readInteger(p, id, wid);
    break;
  case GET_HTABLE_REPLY:
    p += readInteger(p, id, addr, status);
    break;
#endif
  case UPDATE_MEM_STATS:
    p += readInteger(p, wid, size, free);
    break;
  case FETCH_MEM_STATS_REPLY:
  case BROADCAST_MEM_STATS:
    p += readInteger(p, size);
    ptr = const_cast<char *>(p);
    len = strlen((char *)ptr);
    break;
  case GET:
    p += readInteger(p, id, wid, key);
    break;
  /* add ergeda add */
  case SET_CACHE:
    p += readInteger(p, id, wid, addr, size, flag, arg);
    break;
  case SET_CACHE_REPLY:
    p += readInteger(p, id, wid);
    break;
  case RM_READ:
  case JUST_READ:
  case WE_READ:
    p += readInteger(p, id, wid, addr, size, ptr);
    break;
  case READ_TYPE:
    p += readInteger(p, id, wid, addr);
    break;
  case JUST_WRITE:
  case TYPE_REPLY:
  case WE_WRITE:
    p += readInteger(p, id, wid, addr, size, flag);
    ptr = const_cast<char *>(p);
    len = size;
    break;
  case RM_WRITE:
    p += readInteger(p, id, wid, addr, size, flag, next);
    ptr = const_cast<char *>(p);
    len = size;
    break;
  case RM_FORWARD:
  case WE_INV:
    p += readInteger(p, id, wid, addr);
    break;
  case TEST_RDMA:
    p += readInteger(p, id, wid, addr, ptr);
    break;

  /* add ergeda add */
  case PUT:
  case GET_REPLY:
    p += readInteger(p, id, wid, key, size);
    ptr = const_cast<char *>(p);
    len = size;
    break;
  case FETCH_MEM_STATS:
    p += readInteger(p, wid);
    break;

  case MALLOC:
    // p += readInteger(p, id, wid, size, flag);
    /* add ergeda add */
    p += readInteger(p, id, wid, size, flag, arg);
    /* add ergeda add */
    break;
  case MALLOC_REPLY:
    p += readInteger(p, id, wid, addr, status);
    break;
  case FREE:
    p += readInteger(p, id, wid, addr);
    break;
#ifdef NOCACHE
  case WRITE:
  case READ_REPLY:
    p += readInteger(p, id, wid, addr, size, flag);
    ptr = const_cast<char *>(p);
    len = size;
    break;
  case WLOCK:
  case RLOCK:
  case UNLOCK:
#else // else NOCACHE
  case WRITE:
#endif
  {
#ifdef GFUNC_SUPPORT
    int gid = 0;
    p += readInteger(p, id, wid, addr, size, ptr, flag, gid, arg);
    gfunc = GetGFunc(gid);
    epicLog(LOG_DEBUG, "deser gid = %d, gfunc = %ld", gid, gfunc);
    if (!gfunc)
      epicAssert(!(flag & GFUNC));
#ifdef SELECTIVE_CACHING
    if (flag & NOT_CACHE && !(flag & GFUNC))
    {
      ptr = const_cast<char *>(p);
      len = size;
    }
#endif

#else
    p += readInteger(p, id, wid, addr, size, ptr, flag);
#ifdef SELECTIVE_CACHING
    if (flag & NOT_CACHE)
    {
      ptr = const_cast<char *>(p);
      len = size;
    }
#endif
#endif
    break;
  }
  case WRITE_PERMISSION_ONLY:
  {
#ifdef GFUNC_SUPPORT
    int gid = 0;
    p += readInteger(p, id, wid, addr, size, ptr, flag, gid, arg);
    gfunc = GetGFunc(gid);
    epicLog(LOG_DEBUG, "deser gid = %d, gfunc = %ld", gid, gfunc);
    if (!gfunc)
      epicAssert(!(flag & GFUNC));
#else
    p += readInteger(p, id, wid, addr, size, ptr, flag);
#endif
    break;
  }
  case READ:
  case FETCH_AND_SHARED:
  case FETCH_AND_INVALIDATE:
  case INVALIDATE:
  case InitAcquire:
    p += readInteger(p, id, wid, addr, size, ptr, flag);
    break;
  case READ_FORWARD:
  case WRITE_FORWARD:
  case INVALIDATE_FORWARD:
  case WRITE_PERMISSION_ONLY_FORWARD:
    p += readInteger(p, id, wid, addr, size, ptr, flag, pid, pwid);
    break;
#ifndef NOCACHE
  case READ_REPLY:
#else
  case RLOCK_REPLY:
  case WLOCK_REPLY:
#ifndef ASYNC_UNLOCK
  case UNLOCK_REPLY:
#endif
#endif
    p += readInteger(p, id, wid, status);
    break;
  case WRITE_REPLY:
    int c;
    p += readInteger(p, id, wid, status, c);
    counter = c;
    break;
  case ACTIVE_INVALIDATE:
  case WRITE_BACK:
    p += readInteger(p, id, wid, addr, ptr);
    break;
    /* add wpq add */
  case ChangeSubLog:
    p += readInteger(p, addr, id, wid, pid, pwid, flagSub1, flagSub2);
    break;
  case writeshared_READ:
    p += readInteger(p, id, wid, addr, size, ptr, flag, pid, pwid);
    break;
  /* add wpq add */
  default:
    epicLog(LOG_WARNING, "unrecognized op code %d", op);
    break;
  }

  len += p - buf;
  return 0;
}

WorkRequest::WorkRequest(WorkRequest &wr)
{
  // memcpy(this, &wr, sizeof(WorkRequest));
  id = wr.id; // identifier of the work request

  pid = wr.pid;   // identifier of the parent work request (used for FORWARD request)
  pwid = wr.pwid; // identifier of the parent worker
  op = wr.op;

  key = wr.key;
  addr = wr.addr;
  free = wr.free;
  size = wr.size;
  status = wr.status;
  /* add wpq add */

  flagSub1 = wr.flagSub1;
  flagSub2 = wr.flagSub2;
  /* add wpq add */

  flag = wr.flag;
  ptr = wr.ptr;
  fd = wr.fd;
#if !defined(USE_PIPE_W_TO_H) || !defined(USE_PIPE_H_TO_W)
  notify_buf = wr.notify_buf;
#endif
#ifdef USE_PTHREAD_COND
  cond_lock = wr.cond_lock;
  cond = wr.cond;
#endif
  wid = wr.wid;
  counter.store(wr.counter);
  parent = wr.parent;
  next = wr.next;
  dup = wr.dup;

  is_cache_hit_ = wr.is_cache_hit_;
  epicAssert(*this == wr);
  /*
   * LOCAL_REQUEST flag is the only thing that is not copied!
   * this is mainly used for debug
   * can remove after mature
   */
  long mask = ~LOCAL_REQUEST;
  this->flag &= mask;
#ifdef GFUNC_SUPPORT
  gfunc = wr.gfunc;
  arg = wr.arg;
  if (flag & GFUNC)
    epicAssert(gfunc);
#endif
}

bool WorkRequest::operator==(const WorkRequest &wr)
{
  return wr.addr == this->addr && wr.counter == this->counter && wr.fd == this->fd && wr.flag == this->flag && wr.free == this->free && wr.id == this->id && wr.next == this->next && wr.op == this->op && wr.parent == this->parent && wr.pid == this->pid && wr.ptr == this->ptr && wr.pwid == this->pwid && wr.size == this->size && wr.status == this->status && wr.wid == this->wid;
}

WorkRequest::~WorkRequest()
{
}
