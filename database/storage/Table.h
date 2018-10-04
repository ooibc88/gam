#ifndef __DATABASE_STORAGE_TABLE_H__
#define __DATABASE_STORAGE_TABLE_H__

#include <iostream>

#include "Record.h"
#include "RecordSchema.h"
#include "Meta.h"
#include "HashIndex.h"
#include "Profiler.h"

namespace Database {
class Table : public GAMObject{
public:
  Table() {
    schema_ptr_ = nullptr;
    primary_index_ = nullptr;
    secondary_indexes_ = nullptr;
  }
  ~Table() {
    if (primary_index_) {
      delete primary_index_;
      primary_index_ = nullptr;
    }
  }

  void Init(size_t table_id, RecordSchema* schema_ptr, GAlloc* gallocator) {
    table_id_ = table_id;
    schema_ptr_ = schema_ptr;
    secondary_count_ = 0;
    secondary_indexes_ = nullptr;
    
    primary_index_ = new HashIndex();
    primary_index_->Init(kHashIndexBucketHeaderNum, gallocator);
  }

  // return false if the key exists in primary index already
  bool InsertRecord(const IndexKey* keys, size_t key_num, 
      const GAddr& data_addr, GAlloc* gallocator, size_t thread_id) {
    assert(key_num == secondary_count_ + 1);
    return primary_index_->InsertRecord(keys[0], data_addr, 
        gallocator, thread_id);
  }

  GAddr SearchRecord(const IndexKey& key, GAlloc* gallocator, 
      size_t thread_id) {
    return primary_index_->SearchRecord(key, 
        gallocator, thread_id);
  }

  void ReportTableSize() const {
    uint64_t size = primary_index_->GetRecordCount()
        * schema_ptr_->GetSchemaSize();
    std::cout << "table_id=" << table_id_ << ", size="
              << size * 1.0 / 1000 / 1000 << "MB" << std::endl;
  }

  size_t GetTableId() const {
    return table_id_;
  }
  size_t GetSecondaryCount() const {
    return secondary_count_;
  }
  RecordSchema* GetSchema() {
    return schema_ptr_;
  }
  size_t GetSchemaSize() const {
    return schema_ptr_->GetSchemaSize();
  }
  HashIndex* GetPrimaryIndex() {
    return primary_index_;
  }

  virtual void Serialize(const GAddr& addr, GAlloc *gallocator) {
    size_t off = 0;
    gallocator->Write(addr, off, &table_id_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Write(addr, off, &secondary_count_, sizeof(size_t));
    off += sizeof(size_t);
    GAddr cur_addr = GADD(addr, off);
    schema_ptr_->Serialize(cur_addr, gallocator);
    cur_addr = GADD(cur_addr, schema_ptr_->GetSerializeSize()) ;
    primary_index_->Serialize(cur_addr, gallocator);
  }
  
  virtual void Deserialize(const GAddr& addr, GAlloc *gallocator) {
    size_t off = 0;
    gallocator->Read(addr, off, &table_id_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Read(addr, off, &secondary_count_, sizeof(size_t));
    off += sizeof(size_t);
    GAddr cur_addr = GADD(addr, off);
    schema_ptr_ = new RecordSchema(table_id_);
    schema_ptr_->Deserialize(cur_addr, gallocator);
    cur_addr = GADD(cur_addr, schema_ptr_->GetSerializeSize());
    primary_index_ = new HashIndex();
    primary_index_->Deserialize(cur_addr, gallocator);
  }
    
  static size_t GetSerializeSize() {
    size_t ret = sizeof(size_t) * 2;
    ret += RecordSchema::GetSerializeSize();
    ret += HashIndex::GetSerializeSize();
    return ret;
  }

 private:
  size_t table_id_;
  size_t secondary_count_;

  RecordSchema *schema_ptr_;
  HashIndex *primary_index_;
  HashIndex **secondary_indexes_; // Currently disabled
};
}
#endif
