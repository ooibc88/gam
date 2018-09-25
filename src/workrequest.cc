// Copyright (c) 2018 The GAM Authors 

#include <cstring>
#include "workrequest.h"

#include "chars.h"
#include "log.h"

int WorkRequest::Ser(char* buf, int& len) {
  wtype lop = static_cast<wtype>(op);
  stype lstatus = static_cast<stype>(status);

  switch(op) {
    case FETCH_MEM_STATS:
      len = appendInteger(buf, op);
      break;
    case UPDATE_MEM_STATS:
      len = appendInteger(buf, op, size, free);
      break;
    case FETCH_MEM_STATS_REPLY:
    case BROADCAST_MEM_STATS:
      len = appendInteger(buf, op, size);
      memcpy(buf + len, ptr, strlen((char*)ptr));
      len += strlen((char*)ptr);
      break;
    case GET:
    case KV_GET:
      len = appendInteger(buf, op, id, key);
      break;
    case PUT:
    case KV_PUT:
      len = appendInteger(buf, op, id, key, size);
      memcpy(buf+len, ptr, size);
      len += size;
      break;
    case GET_REPLY:
      len = appendInteger(buf, op, id, key, size, lstatus);
      if (static_cast<Status>(lstatus) == Status::SUCCESS) {
        memcpy(buf+len, ptr, size);
        len += size;
      }
      break;
    case PUT_REPLY:
      len = appendInteger(buf, op, id, key, lstatus);
      break;

    case FARM_MALLOC:
      //len = sprintf(buf, "%x:%x:%lx:%x:", op, id, size, flag);
      len = appendInteger(buf, lop, id, size);
      break;
    case FARM_MALLOC_REPLY:
      len = appendInteger(buf, lop, id, addr, lstatus);
      break;
    case FARM_READ:
      len = appendInteger(buf, lop, id, addr);
      break;
    case FARM_READ_REPLY:
      len = appendInteger(buf, lop, id, lstatus);
      if (static_cast<Status>(lstatus) == Status::SUCCESS)
      {
        memcpy(buf + len, this->ptr, this->size);
        len += this->size;
      }
      break;
    case PREPARE:
    case VALIDATE:
      /* for prepare and validate, we are no longer interested at addr as it
       * already exists in the message
       **/
      len = appendInteger(buf, lop, id, nobj);
      break;
    case COMMIT:
    case ABORT:
      len = appendInteger(buf, lop, id);
      break;
    case VALIDATE_REPLY:
    case PREPARE_REPLY:
    case ACKNOWLEDGE:
      len = appendInteger(buf, lop, id, lstatus);
      break;

    default:
      epicLog(LOG_WARNING, "unrecognized op code");
      break;
  }
  buf[len] = '\0';
  return 0;
}

int WorkRequest::Deser (const char* buf, int& len) {
  int ret;
  size = len = 0;
  char* p = (char*)buf;
  wtype lop;
  p += readInteger(p, lop);
  op = static_cast<Work>(lop);
  stype s;

  switch(op) {
    case FETCH_MEM_STATS:
      break;
    case UPDATE_MEM_STATS:
      p += readInteger(p, size, free);
      break;
    case FETCH_MEM_STATS_REPLY:
    case BROADCAST_MEM_STATS:
      p += readInteger(p, size);
      ptr = p;
      len = strlen((char*)ptr);
      break;
    case GET:
    case KV_GET:
      p += readInteger(p, id, key);
      break;
    case GET_REPLY:
      p += readInteger(p, id, key, size, s);
      this->status = s;
      if (status == SUCCESS)
        memcpy(ptr, p, size);
      len = size;
      break;
    case PUT:
    case KV_PUT:
      p += readInteger(p, id, key, size);
      ptr = p;
      len = size;
      break;
    case PUT_REPLY:
      p += readInteger(p, id, key, s);
      status = s;
      break;
    case FARM_MALLOC:
      p += readInteger(p, id, size);
      break;
    case FARM_MALLOC_REPLY:
      p += readInteger(p, id, addr, s);
      status = s;
      break;
    case FARM_READ:
      p += readInteger(p, id, addr);
      break;
    case FARM_READ_REPLY:
      p += readInteger(p, id, s);
      status = s;
      break;
    case PREPARE:
    case VALIDATE:
      p += readInteger(p, id, nobj);
      ptr = p;
      break;
    case COMMIT:
    case ABORT: 
      p += readInteger(p, id);
      break;
    case VALIDATE_REPLY:
    case PREPARE_REPLY:
    case ACKNOWLEDGE:
      p += readInteger(p, id, s);
      status = s;
      break;
    default:
      epicLog(LOG_WARNING, "unrecognized op code %d", op);
      break;
  }

  len += p - buf;

  return 0;
}

WorkRequest::WorkRequest(WorkRequest& wr) {
  memcpy(this, &wr, sizeof(WorkRequest));
  //*this = wr;
}

bool WorkRequest::operator==(const WorkRequest& wr) {
  return wr.addr == this->addr && wr.counter == this->counter && wr.fd == this->fd
    && wr.flag == this->flag && wr.free == this->free && wr.id == this->id
    && wr.next == this->next && wr.op == this->op && wr.parent == this->parent
    && wr.pid == this->pid && wr.ptr == this->ptr && wr.pwid == this->pwid
    && wr.size == this->size && wr.status == this->status && wr.wid == this->wid;
}

const char* workToStr(Work op) {
  static char s[100];
  memset(s, 0, 100);
  switch(op) {
    case FARM_MALLOC:
      strcpy(s, "FARM_MALLOC");
      break;
    case FARM_MALLOC_REPLY:
      strcpy(s, "FARM_MALLOC_REPLY");
      break;
    case FARM_READ:
      strcpy(s, "FARM_READ");
      break;
    case FARM_READ_REPLY:
      strcpy(s, "FARM_READ_REPLY");
      break;
    case PREPARE:
      strcpy(s, "FARM_PREPARE");
      break;
    case PREPARE_REPLY:
      strcpy(s, "FARM_PREPARE_REPLY");
      break;
    case VALIDATE:
      strcpy(s, "FARM_VALIDATE");
      break;
    case VALIDATE_REPLY:
      strcpy(s, "FARM_VALIDATE_REPLY");
      break;
    case COMMIT:
      strcpy(s, "FARM_COMMIT");
      break;
    case ABORT:
      strcpy(s, "FARM_ABORT");
      break;
    case ACKNOWLEDGE:
      strcpy(s, "FARM_ACKNOWLEDGE");
      break;
    case BROADCAST_MEM_STATS:
      strcpy(s, "BROADCAST_MEM_STATS");
      break;
    case FETCH_MEM_STATS:
      strcpy(s, "FETCH_MEM_STATS");
      break;
    case FETCH_MEM_STATS_REPLY:
      strcpy(s, "FETCH_MEM_STATS_REPLY");
      break;
    case UPDATE_MEM_STATS:
      strcpy(s, "UPDATE_MEM_STATS");
      break;
    case PUT:
      strcpy(s, "PUT");
      break;
    case PUT_REPLY:
      strcpy(s, "PUT_REPLY");
      break;
    case GET_REPLY:
      strcpy(s, "GET_REPLY");
      break;
    case GET:
      strcpy(s, "GET");
      break;
  }

  return s;
}

WorkRequest::~WorkRequest() {}
