// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_BENCHMARK_SOURCE_H__
#define __DATABASE_BENCHMARK_SOURCE_H__

#include <iostream>

#include "Meta.h"
#include "IORedirector.h"
#include "TimeMeasurer.h"

namespace Database {
class BenchmarkSource {
 public:
  BenchmarkSource(IORedirector* redirector, size_t num_txn, size_t source_type,
                  size_t thread_count, size_t dist_ratio)
      : redirector_ptr_(redirector),
        num_txn_(num_txn),
        source_type_(source_type),
        dist_ratio_(dist_ratio),
        thread_count_(thread_count) {
  }
  ~BenchmarkSource() {
  }

  void Start() {
    TimeMeasurer timer;
    timer.StartTimer();
    StartGeneration();
    timer.EndTimer();
    std::cout << "source elapsed time=" << timer.GetElapsedMilliSeconds()
              << "ms" << std::endl;
  }

  virtual void StartGeneration() = 0;

 protected:
  IORedirector* redirector_ptr_;
  const size_t num_txn_;
  const size_t dist_ratio_;
  const size_t source_type_;
  const size_t thread_count_;
};
}

#endif
