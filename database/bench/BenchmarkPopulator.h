// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_BENCHMARK_POPULATOR_H__
#define __DATABASE_BENCHMARK_POPULATOR_H__

#include <iostream>

#include "StorageManager.h"
#include "Meta.h"
#include "TimeMeasurer.h"

namespace Database {
class BenchmarkPopulator {
 public:
  BenchmarkPopulator(StorageManager *storage_manager)
      : storage_manager_(storage_manager) {
  }

  virtual ~BenchmarkPopulator() {
  }

  void Start() {
    std::cout << "start population" << std::endl;
    TimeMeasurer timer;
    timer.StartTimer();
    StartPopulate();
    timer.EndTimer();
    std::cout << "populate elapsed time=" << timer.GetElapsedMilliSeconds()
              << "ms" << std::endl;
  }

  virtual void StartPopulate() = 0;

 private:
  BenchmarkPopulator(const BenchmarkPopulator &);
  BenchmarkPopulator& operator=(const BenchmarkPopulator &);

 protected:
  StorageManager *storage_manager_;
};
}

#endif
