// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_STORAGE_RECORDS_H__
#define __DATABASE_STORAGE_RECORDS_H__

#include "Record.h"

namespace Database {
class Records {
 public:
  Records(size_t max_size)
      : max_size_(max_size) {
    record_count_ = 0;
    records_ = new Record*[max_size_];
  }
  ~Records() {
    delete[] records_;
    records_ = nullptr;
  }
  void InsertRecord(Record* record) {
    assert(record_count_ < max_size_);
    records_[record_count_++] = record;
  }
  void Clear() {
    record_count_ = 0;
  }
 public:
  size_t max_size_;
  size_t record_count_;
  Record **records_;
};
}

#endif
