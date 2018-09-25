// Copyright (c) 2018 The GAM Authors 

#ifndef LOGGING_H
#define LOGGING_H 

#include <fstream>
#include <string>
#include <atomic>
#include <future>
#include <thread>

#include "structure.h"
//#include "gallocator.h"

class Log {
  public:
    friend class Worker;
    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;
    void logWrite(GAddr, Size, const void*);
    void logOwner(int,  GAddr);

  private:
    void* base_;
    std::atomic_uint_fast32_t spos_, epos_;
    std::atomic_bool running_;
    char* buf_;
    int fd_;
    std::future<void> async_;
    std::thread::id thread_;

    void* toLocal(GAddr addr) {
      return TO_LOCAL(addr, base_);
    }

    void write();
    int writeToBuf(void*, Size, int);
    int reserve(Size);
    Log(void*);
    ~Log();
};

#endif
