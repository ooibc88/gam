// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE__TRANSACTION_EXECUTOR_H__
#define __DATABASE__TRANSACTION_EXECUTOR_H__

#include "StorageManager.h"
#include "IORedirector.h"
#include "Meta.h"
#include "TxnParam.h"
#include "TimeMeasurer.h"
#include "StoredProcedure.h"
#include "Profiler.h"
#include "PerfStatistics.h"
#include <iostream>
#include <unordered_map>
#include <boost/thread.hpp>
#include <atomic>
#include <xmmintrin.h>

namespace Database {
class TransactionExecutor {
 public:
  TransactionExecutor(IORedirector* const redirector, 
      StorageManager *storage_manager, size_t thread_count)
      : redirector_ptr_(redirector),
        storage_manager_(storage_manager),
        thread_count_(thread_count) {
    is_begin_ = false;
    is_finish_ = false;
    total_count_ = 0;
    total_abort_count_ = 0;
    is_ready_ = new volatile bool[thread_count_];
    for (size_t i = 0; i < thread_count_; ++i) {
      is_ready_[i] = false;
    }
    memset(&time_lock_, 0, sizeof(time_lock_));
  }
  ~TransactionExecutor() {
    delete[] is_ready_;
    is_ready_ = NULL;
  }

  virtual void Start() {
    PrepareProcedures();
    ProcessQuery();
  }

  PerfStatistics &GetPerfStatistics() {
    return perf_statistics_;
  }

 private:
  virtual void PrepareProcedures() = 0;

  virtual void ProcessQuery() {
    std::cout << "start process query" << std::endl;
    boost::thread_group thread_group;
    for (size_t i = 0; i < thread_count_; ++i) {
      // can bind threads to cores here
      thread_group.create_thread(
          boost::bind(&TransactionExecutor::ProcessQueryThread, this, i));
    }
    bool is_all_ready = true;
    while (1) {
      for (size_t i = 0; i < thread_count_; ++i) {
        if (is_ready_[i] == false) {
          is_all_ready = false;
          break;
        }
      }
      if (is_all_ready == true) {
        break;
      }
      is_all_ready = true;
    }
    // epoch generator.
    std::cout << "start processing..." << std::endl;
    is_begin_ = true;
    start_timestamp_ = timer_.GetTimePoint();
    thread_group.join_all();
    long long elapsed_time = timer_.CalcMilliSecondDiff(start_timestamp_,
                                                        end_timestamp_);
    double throughput = total_count_ * 1.0 / elapsed_time;
    double per_core_throughput = throughput / thread_count_;
    std::cout << "execute_count=" << total_count_ << ", abort_count="
              << total_abort_count_ << ", abort_rate="
              << total_abort_count_ * 1.0 / (total_count_ + 1) << std::endl;
    std::cout << "elapsed time=" << elapsed_time << "ms.\nthroughput="
              << throughput << "K tps.\nper-core throughput="
              << per_core_throughput << "K tps." << std::endl;

    perf_statistics_.total_count_ = total_count_;
    perf_statistics_.total_abort_count_ = total_abort_count_;
    perf_statistics_.thread_count_ = thread_count_;
    perf_statistics_.elapsed_time_ = elapsed_time;
    perf_statistics_.throughput_ = throughput;
  }

  virtual void ProcessQueryThread(const size_t& thread_id) {
    //std::cout << "start thread " << thread_id << std::endl;
    std::vector<ParamBatch*> &execution_batches = 
      *(redirector_ptr_->GetParameterBatches(thread_id));

    TransactionManager *txn_manager = new TransactionManager(
        storage_manager_, this->thread_count_, thread_id);
    StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
    for (auto &entry : registers_) {
      procedures[entry.first] = entry.second();
      procedures[entry.first]->SetTransactionManager(txn_manager);
    }

    is_ready_[thread_id] = true;
    while (is_begin_ == false)
      ;
    int count = 0;
    int abort_count = 0;
    uint32_t backoff_shifts = 0;
    CharArray ret;
    ret.char_ptr_ = new char[1024];
    for (auto& tuples : execution_batches) {
      for (size_t idx = 0; idx < tuples->size(); ++idx) {
        TxnParam* tuple = tuples->get(idx);
        // begin txn
        PROFILE_TIME_START(thread_id, TXN_EXECUTE);
        ret.size_ = 0;
        if (procedures[tuple->type_]->Execute(tuple, ret) == false) {
          ret.size_ = 0;
          ++abort_count;
          if (is_finish_ == true) {
            total_count_ += count;
            total_abort_count_ += abort_count;
            PROFILE_TIME_END(thread_id, TXN_EXECUTE);
            //txn_manager->CleanUp();
            return;
          }PROFILE_TIME_START(thread_id, TXN_ABORT);
#if defined(BACKOFF)						
          if (backoff_shifts < 63) {
            ++backoff_shifts;
          }
          uint64_t spins = 1UL << backoff_shifts;

          spins *= 100;
          while (spins) {
            _mm_pause();
            --spins;
          }
#endif
          while (procedures[tuple->type_]->Execute(tuple, ret) == false) {

            ret.size_ = 0;
            ++abort_count;
            if (is_finish_ == true) {
              total_count_ += count;
              total_abort_count_ += abort_count;
              PROFILE_TIME_END(thread_id, TXN_ABORT);PROFILE_TIME_END(
                  thread_id, TXN_EXECUTE);
              //txn_manager->CleanUp();
              return;
            }
#if defined(BACKOFF)
            uint64_t spins = 1UL << backoff_shifts;
            spins *= 100;
            while (spins) {
              _mm_pause();
              --spins;
            }
#endif
          }PROFILE_TIME_END(thread_id, TXN_ABORT);
        } else {
#if defined(BACKOFF)
          backoff_shifts >>= 1;
#endif
        }
        ++count;
        PROFILE_TIME_END(thread_id, TXN_EXECUTE);
        if (is_finish_ == true) {
          total_count_ += count;
          total_abort_count_ += abort_count;
          //txn_manager->CleanUp();
          return;
        }
      }
    }
    time_lock_.lock();
    end_timestamp_ = timer_.GetTimePoint();
    is_finish_ = true;
    time_lock_.unlock();
    total_count_ += count;
    total_abort_count_ += abort_count;
    //txn_manager->CleanUp();
    return;
  }

 protected:
  size_t thread_count_;
  StorageManager *storage_manager_;
  IORedirector* const redirector_ptr_;

  std::unordered_map<size_t, std::function<StoredProcedure*()>> registers_;
  std::unordered_map<size_t, std::function<void(StoredProcedure*)>> deregisters_;

 private:
  // perf measurement
  TimeMeasurer timer_;
  system_clock::time_point start_timestamp_;
  system_clock::time_point end_timestamp_;
  boost::detail::spinlock time_lock_;
  // multi-thread util
  volatile bool *is_ready_;
  volatile bool is_begin_;
  volatile bool is_finish_;
  // profile count
  std::atomic<size_t> total_count_;
  std::atomic<size_t> total_abort_count_;

  PerfStatistics perf_statistics_;
};
}

#endif
