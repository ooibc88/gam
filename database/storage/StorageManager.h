#ifndef __DATABASE_STORAGE_STORAGE_MANAGER_H__
#define __DATABASE_STORAGE_STORAGE_MANAGER_H__

#include <iostream>
#include <vector>

#include "Table.h"

namespace Database {
class StorageManager : public GAMObject{
public:
  StorageManager() {
    tables_ = nullptr;
    table_count_ = 0;
  }
  ~StorageManager() {
    if (tables_) {
      assert(table_count_ > 0);
      for (size_t i = 0; i < table_count_; ++i) {
        delete tables_[i];
        tables_[i] = nullptr;
      } 
      delete[] tables_;
      tables_ = nullptr;
    }
  }

  void RegisterTables(const std::vector<RecordSchema*>& schemas, 
      GAlloc* gallocator) {
    table_count_ = schemas.size();
    assert(table_count_ < kMaxTableNum);
    tables_ = new Table*[table_count_];
    for (size_t i = 0; i < table_count_; ++i) {
      Table* table = new Table();
      table->Init(i, schemas[i], gallocator);
      tables_[i] = table;
    }
  }

  size_t GetTableCount() const {
    return table_count_;
  }

  virtual void Serialize(const GAddr& addr, GAlloc *gallocator) {
    gallocator->Write(addr, &table_count_, sizeof(size_t));
    GAddr cur_addr = GADD(addr, sizeof(size_t));
    for (size_t i = 0; i < table_count_; ++i) {
      tables_[i]->Serialize(cur_addr, gallocator);
      cur_addr = GADD(cur_addr, Table::GetSerializeSize());
    }
  }
    
  virtual void Deserialize(const GAddr& addr, GAlloc *gallocator) {
    gallocator->Read(addr, &table_count_, sizeof(size_t));
    GAddr cur_addr = GADD(addr, sizeof(size_t));
    tables_ = new Table*[table_count_];
    for (size_t i = 0; i < table_count_; ++i) {
      Table* table = new Table();
      table->Deserialize(cur_addr, gallocator);
      tables_[i] = table;
      cur_addr = GADD(cur_addr, Table::GetSerializeSize());
    }
  }

  static size_t GetSerializeSize() {
    return sizeof(size_t) + kMaxTableNum * Table::GetSerializeSize();
  }

public:
  Table **tables_;

private:
  size_t table_count_;
};
}
#endif
