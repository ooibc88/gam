// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN__STORED_PROCEDURE_H__
#define __DATABASE_TXN__STORED_PROCEDURE_H__

#include "StorageManager.h"
#include "TransactionManager.h"
#include "TxnContext.h"
#include "Meta.h"

namespace Database {
#define DB_QUERY(statement) \
	if (transaction_manager_->statement == false) return false;

#define DB_QUERY_CALLBACK(statement, callback) \
	if (transaction_manager_->statement == false) { callback; return false; }

class StoredProcedure {
 public:
  StoredProcedure() {
    context_.txn_type_ = 0;
    thread_id_ = 0;
  }
  StoredProcedure(const size_t &txn_type) {
    context_.txn_type_ = txn_type;
    thread_id_ = 0;
  }
  virtual ~StoredProcedure() {
  }

  void SetTransactionManager(TransactionManager *transaction_manager) {
    transaction_manager_ = transaction_manager;
    thread_id_ = transaction_manager_->GetThreadId();
  }
  
  virtual bool Execute(TxnParam *param, CharArray &ret) {
    return true;
  }

 private:
  StoredProcedure(const StoredProcedure&);
  StoredProcedure& operator=(const StoredProcedure&);

 protected:
  TxnContext context_;
  TransactionManager *transaction_manager_;
  size_t thread_id_;
};
}

#endif
