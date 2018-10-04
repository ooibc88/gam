#if defined(ST)
#include "TransactionManager.h"

namespace Database {
  bool TransactionManager::InsertRecord(TxnContext* context, size_t table_id, const IndexKey* keys, 
      size_t key_num, Record *record, const GAddr& record_addr) {
    Access *access = access_list_.NewAccess();
    access->access_type_ = INSERT_ONLY;
    access->access_record_ = record;
    access->access_addr_ = record_addr;
    return true;
  }

  bool TransactionManager::SelectRecordCC(TxnContext* context, size_t table_id, 
      Record *&record, const GAddr& record_addr, AccessType access_type) {
    RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
    record = new Record(schema_ptr);
    record->Deserialize(record_addr, gallocators[thread_id_]);
    Access *access = access_list_.NewAccess();
    access->access_type_ = access_type;
    access->access_record_ = record;
    access->access_addr_ = record_addr;
    return true;
  }

  bool TransactionManager::CommitTransaction(TxnContext* context, TxnParam* param, CharArray& ret_str) {
    for (size_t i = 0; i < access_list_.access_count_; ++i) {
      Access *access = access_list_.GetAccess(i);
      if (access->access_type_ == INSERT_ONLY) {
        gallocators[thread_id_]->Free(access->access_addr_);
      }
      delete access->access_record_;
      access->access_record_ = nullptr;
      access->access_addr_ = Gnullptr;
    }
    access_list_.Clear();
    return true;
  }

  void TransactionManager::AbortTransaction() {
    assert(false);
  }
}

#endif
