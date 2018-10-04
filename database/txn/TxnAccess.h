// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_TXN_ACCESS_H__
#define __DATABASE_TXN_TXN_ACCESS_H__

#include "Record.h"
#include "gallocator.h"

namespace Database {
struct Access {
  Access()
      : access_record_(nullptr), access_addr_(Gnullptr) {
  }
  AccessType access_type_;
  Record *access_record_;
  GAddr access_addr_;
};

template<int N>
class AccessList {
 public:
  AccessList()
      : access_count_(0) {
  }

  Access *NewAccess() {
    assert(access_count_ < N);
    Access *ret = &(accesses_[access_count_]);
    ++access_count_;
    return ret;
  }

  Access *GetAccess(const size_t &index) {
    return &(accesses_[index]);
  }

  void Clear() {
    access_count_ = 0;
  }

 public:
  size_t access_count_;
 private:
  Access accesses_[N];
};
}

#endif
