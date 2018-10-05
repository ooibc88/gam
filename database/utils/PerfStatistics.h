// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_UTILS_PERFORMANCE_STATISTICS_H__
#define __DATABASE_UTILS_PERFORMANCE_STATISTICS_H__

#include <iostream>
#include <cstdio>

namespace Database {
struct PerfStatistics {
  PerfStatistics() {
    total_count_ = 0;
    total_abort_count_ = 0;
    thread_count_ = 0;
    elapsed_time_ = 0;
    throughput_ = 0.0;

    agg_total_count_ = 0;
    agg_thread_count_ = 0;
    agg_total_abort_count_ = 0;
    agg_elapsed_time_ = 0;
    agg_node_num_ = 0;
    longest_elapsed_time_ = 0;
    agg_throughput_ = 0.0;
  }
  void PrintAgg() {
    std::cout
        << "==================== perf statistics summary ===================="
        << std::endl;
    double abort_rate = agg_total_abort_count_ * 1.0 / (agg_total_count_ + 1);
    printf(
        "agg_total_count\t%lld\nagg_total_abort_count\t%lld\nabort_rate\t%lf\n",
        agg_total_count_, agg_total_abort_count_, abort_rate);
    printf(
        "per_node_elapsed_time\t%lf\ntotal_throughput\t%lf\nper_node_throughput\t%lf\nper_core_throughput\t%lf\n",
        agg_elapsed_time_ * 1.0 / agg_node_num_, agg_throughput_,
        agg_throughput_ / agg_node_num_, agg_throughput_ / agg_thread_count_);

    /*std::cout << "agg_total_count=" << agg_total_count_ <<", agg_total_abort_count=" << agg_total_abort_count_ <<", abort_rate=" << abort_rate << std::endl;
     std::cout << "per node elapsed time=" << agg_elapsed_time_ * 1.0 / agg_node_num_ << "ms." << std::endl;
     std::cout << "total throughput=" << agg_throughput_ << "K tps,per node throughput=" 
     << agg_throughput_ / agg_node_num_ << "K tps." << ",per core throughput=" << agg_throughput_ / agg_thread_count_ << std::endl;*/
    std::cout << "==================== end ====================" << std::endl;
  }
  void Print() {
    std::cout << "total_count=" << total_count_ << ",total_abort_count="
              << total_abort_count_ << ",throughput=" << throughput_
              << ",thread_count_=" << thread_count_ << ",elapsed_time="
              << elapsed_time_ << std::endl;
  }
  void Aggregate(const PerfStatistics& obj) {
    agg_total_count_ += obj.total_count_;
    agg_total_abort_count_ += obj.total_abort_count_;
    agg_throughput_ += obj.throughput_;
    agg_thread_count_ += obj.thread_count_;
    agg_elapsed_time_ += obj.elapsed_time_;
    agg_node_num_++;
  }

  long long total_count_;
  long long total_abort_count_;
  long long thread_count_;
  long long elapsed_time_;  // in milli seconds
  double throughput_;

  long long agg_total_count_;
  long long agg_total_abort_count_;
  double agg_throughput_;
  long long agg_thread_count_;
  long long agg_elapsed_time_;
  long long agg_node_num_;
  long long longest_elapsed_time_;
};
}

#endif
