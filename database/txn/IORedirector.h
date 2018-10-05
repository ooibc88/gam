// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_IO_REDIRECTOR_H__
#define __DATABASE_TXN_IO_REDIRECTOR_H__

#include <vector>
#include "TxnParam.h"

namespace Database {
class IORedirector {
 public:
  IORedirector(const size_t &thread_count)
      : thread_count_(thread_count),
        curr_thread_id_(0) {
    input_batches_ = new std::vector<ParamBatch*>[thread_count];
  }
  ~IORedirector() {
    delete[] input_batches_;
    input_batches_ = NULL;
  }

  std::vector<ParamBatch*> *GetParameterBatches() {
    return input_batches_;
  }
  std::vector<ParamBatch*> *GetParameterBatches(const size_t &thread_id) {
    return &(input_batches_[thread_id]);
  }
  void PushParameterBatch(ParamBatch *tuples) {
    input_batches_[curr_thread_id_].push_back(tuples);
    curr_thread_id_ = (curr_thread_id_ + 1) % thread_count_;
  }

 private:
  IORedirector(const IORedirector &);
  IORedirector& operator=(const IORedirector &);

 protected:
  std::vector<ParamBatch*> *input_batches_;
  size_t thread_count_;
  size_t curr_thread_id_;
};
}

#endif
