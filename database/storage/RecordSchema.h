// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_STORAGE_RECORD_SCHEMA_H__
#define __DATABASE_STORAGE_RECORD_SCHEMA_H__

#include <vector>
#include <cassert>
#include <cstring>
#include "ColumnInfo.h"
#include "GAMObject.h"

namespace Database {
class RecordSchema : public GAMObject{
public:
  RecordSchema(size_t table_id)
      : table_id_(table_id) {
    column_count_ = 0;
    column_offset_ = 0;
    primary_column_num_ = 0;
    secondary_index_num_ = 0;
    partition_column_num_ = 0;
    columns_ = nullptr;
  }
  ~RecordSchema() {
    if (columns_) {
      delete[] columns_;
      columns_ = nullptr;
    }
  }

  void InsertColumns(const std::vector<ColumnInfo*> &columns) {
    column_count_ = columns.size();
    assert(column_count_ <= kMaxColumnNum);
    columns_ = new ColumnInfo[column_count_];
    for (size_t i = 0; i < column_count_; ++i) {
      ColumnInfo column_info(*columns.at(i));
      column_info.column_offset_ = column_offset_;
      column_offset_ += column_info.column_size_;
      memcpy(columns_ + i, &column_info, sizeof(ColumnInfo));
    }
  }

  const ValueType GetColumnType(size_t index) const {
    assert(index < column_count_);
    return columns_[index].column_type_;
  }

  const size_t GetColumnOffset(size_t index) const {
    assert(index < column_count_);
    return columns_[index].column_offset_;
  }

  const size_t GetColumnSize(size_t index) const {
    assert(index < column_count_);
    return columns_[index].column_size_;
  }

  void SetPrimaryColumns(const size_t *column_ids, size_t column_num) {
    assert(column_num <= 5);
    primary_column_num_ = column_num;
    memcpy(primary_column_ids_, column_ids,
           primary_column_num_ * sizeof(size_t));
  }
  void AddSecondaryColumns(const size_t *column_ids, const size_t &column_num) {
    assert(column_num <= 5);
    secondary_column_num_[secondary_index_num_] = column_num;
    memcpy(secondary_column_ids_[secondary_index_num_], column_ids,
           column_num * sizeof(size_t));
    secondary_index_num_++;
    assert(secondary_index_num_ <= 3);
  }
  void SetPartitionColumns(const size_t *column_ids, const size_t &column_num) {
    assert(column_num < 5);
    partition_column_num_ = column_num;
    memcpy(partition_column_ids_, column_ids,
           partition_column_num_ * sizeof(size_t));
  }

  const size_t& GetSchemaSize() const {
    return column_offset_;
  }
  const size_t& GetColumnCount() const {
    return column_count_;
  }
  const size_t& GetTableId() const {
    return table_id_;
  }
  const size_t& GetSecondaryCount() const {
    return secondary_index_num_;
  }
  const size_t& GetPartitionCount() const {
    return partition_column_num_;
  }
  const size_t GetMetaColumnId() const {
    return column_count_ - 1;
  }
  
  virtual void Serialize(const GAddr& addr, GAlloc *gallocator) {
    size_t off = 0;
    gallocator->Write(addr, off, &table_id_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Write(addr, off, &column_count_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Write(addr, off, &column_offset_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Write(addr, off, primary_column_ids_, sizeof(size_t)*5);
    off += sizeof(size_t) * 5;
    gallocator->Write(addr, off, &primary_column_num_, sizeof(size_t));
    off += sizeof(size_t);
    for (size_t i = 0; i < 3; ++i) {
      gallocator->Write(addr, off, secondary_column_ids_[i], sizeof(size_t)*5);
      off += sizeof(size_t) * 5;
    }
    gallocator->Write(addr, off, secondary_column_num_, sizeof(size_t)*3);
    off += sizeof(size_t) * 3;
    gallocator->Write(addr, off, &secondary_index_num_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Write(addr, off, partition_column_ids_, sizeof(size_t)*5);
    off += sizeof(size_t) * 5;
    gallocator->Write(addr, off, &partition_column_num_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Write(addr, off, columns_, sizeof(ColumnInfo) * column_count_);
  }
  
  virtual void Deserialize(const GAddr& addr, GAlloc *gallocator) {
    size_t off = 0;
    gallocator->Read(addr, off, &table_id_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Read(addr, off, &column_count_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Read(addr, off, &column_offset_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Read(addr, off, primary_column_ids_, sizeof(size_t)*5);
    off += sizeof(size_t) * 5;
    gallocator->Read(addr, off, &primary_column_num_, sizeof(size_t));
    off += sizeof(size_t);
    for (size_t i = 0; i < 3; ++i) {
      gallocator->Read(addr, off, secondary_column_ids_[i], sizeof(size_t)*5);
      off += sizeof(size_t) * 5;
    }
    gallocator->Read(addr, off, secondary_column_num_, sizeof(size_t)*3);
    off += sizeof(size_t) * 3;
    gallocator->Read(addr, off, &secondary_index_num_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Read(addr, off, partition_column_ids_, sizeof(size_t)*5);
    off += sizeof(size_t) * 5;
    gallocator->Read(addr, off, &partition_column_num_, sizeof(size_t));
    off += sizeof(size_t);
    columns_ = new ColumnInfo[column_count_];
    gallocator->Read(addr, off, columns_, sizeof(ColumnInfo) * column_count_);
  }
  
  static size_t GetSerializeSize() {
    size_t ret = sizeof(size_t) * 3;
    ret += sizeof(size_t) * 5 + sizeof(size_t);
    ret += sizeof(size_t) * 3 * 5 + sizeof(size_t) * 3 + sizeof(size_t);
    ret += sizeof(size_t) * 5 + sizeof(size_t);
    ret += sizeof(ColumnInfo) * kMaxColumnNum; 
    return ret;
  }

private:
  RecordSchema(const RecordSchema&);
  RecordSchema& operator=(const RecordSchema&);

private:
  size_t table_id_;
  size_t column_count_;
  size_t column_offset_;

  // Temporarily disabled
  size_t primary_column_ids_[5];
  size_t primary_column_num_;
  size_t secondary_column_ids_[3][5];
  size_t secondary_column_num_[3];
  size_t secondary_index_num_;
  size_t partition_column_ids_[5];
  size_t partition_column_num_;

  ColumnInfo* columns_;
};
}

#endif
