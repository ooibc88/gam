// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_TXN_CONTEXT_H__
#define __DATABASE_TXN_TXN_CONTEXT_H__

namespace Database {
struct TxnContext {
  TxnContext()
      : txn_type_(0),
        is_ready_only_(false),
        is_dependent_(false) {
  }
  size_t txn_type_;
  bool is_ready_only_;
  bool is_dependent_;
};
}

#endif
