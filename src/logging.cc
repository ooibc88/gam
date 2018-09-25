// Copyright (c) 2018 The GAM Authors 

#include <unistd.h>
#include <fcntl.h>

#include <thread>
#include <iostream>

#include "logging.h"
#include "log.h"

constexpr Size BUF_SIZE = 128 * 1024 * 1024;

static Size distance(int start, int end) {
  return (start < end) ? end - start : end + BUF_SIZE - start;
}

Log::Log(void* base): base_(base)
  , spos_(0)
  , epos_(BUF_SIZE)
  , running_(false)
  , buf_(new char[BUF_SIZE])
  , thread_(std::this_thread::get_id()) 
{
  fd_ = open("/data/caiqc/gam.log", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
}

Log::~Log() {
  close(fd_);
  delete buf_;
}

int Log::reserve(Size sz) {

  bool reserved = false;
  while (!reserved) {
    bool fvalue = false;
    if (distance(spos_, epos_) >= sz) {
      uint_fast32_t spos = spos_.load();
      uint_fast32_t npos = (spos + sz) % BUF_SIZE;
      if (spos_.compare_exchange_strong(spos, npos)) {
        if (distance(spos_, epos_) < BUF_SIZE / 2) {
          if (running_.compare_exchange_strong(fvalue, true)) {
            //epicLog(LOG_WARNING, "start reclaim, running_ = %d", running_ ? 1 : 0);
            async_ = std::async(std::launch::async, &Log::write, this);
          }
        }
        return spos;
      }
    } else {
      if (running_.compare_exchange_strong(fvalue, true)) {
        write();
      }
    }
  }
}

void Log::write() {

  // append [epos, spos) of buf to the log file
  int start = epos_ % BUF_SIZE;
  int end = spos_;

  Size avail = distance(start, end);

  epicLog(LOG_WARNING, " start reclaim: avail = %llu MB", avail >> 20);

  if (start < end) {
    ::write(fd_, buf_ + start, end - start);
  } else {
    ::write(fd_, buf_ + end, BUF_SIZE - end);
    ::write(fd_, buf_, start);
  }

  fsync(fd_);
  epos_.store(end);
  running_.store(false);
}

int Log::writeToBuf(void* ptr, Size size, int spos) {
  if (spos + size < BUF_SIZE) {
    memcpy(spos + buf_, ptr, size);
    return spos + size;
  } else {
    int bytesToEnd = BUF_SIZE - spos;
    memcpy(spos + buf_, ptr, bytesToEnd);
    memcpy(buf_, ptr + bytesToEnd, size - bytesToEnd);
    return size - bytesToEnd;
  }
}

void Log::logWrite(GAddr addr, Size size, const void* content) {

  int sz = size + sizeof addr + sizeof size;
  int spos = reserve(sz);

  // simply memory copy
  void* p = TO_LOCAL(addr, base_);

  spos = writeToBuf(&addr, sizeof(GAddr), spos);
  spos = writeToBuf(&size, sizeof(Size), spos);
  spos = writeToBuf(p, size, spos);

  //if (distance(spos, epos_) < BUF_SIZE / 2) {
  //    if (running_.compare_exchange_strong(FALSE_VALUE, true)) {
  //      async_ = std::async(std::launch::async, &Log::write, this);
  //    }
  //}
}

void Log::logOwner(int node, GAddr addr) {
  Size id = node;
  int sz = sizeof addr + sizeof id;
  int spos = reserve(sz);

  id |= (1ul << (8*(sizeof(Size)) - 1));
  spos = writeToBuf(&addr, sizeof(GAddr), spos);
  spos = writeToBuf(&id, sizeof(id), spos);
}
