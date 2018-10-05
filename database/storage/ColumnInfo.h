// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_STORAGE_COLUMN_INFO_H__  
#define __DATABASE_STORAGE_COLUMN_INFO_H__  

#include <cstring>
#include <cstdint>
#include <cassert>
#include "Meta.h"

namespace Database {
enum ValueType
  : size_t {INT,
  INT8,
  INT16,
  INT32,
  INT64,
  DOUBLE,
  FLOAT,
  VARCHAR,
  META
};

struct MetaColumn {
  bool is_visible_;
  // uint64_t timestamp_;
};

const size_t kIntSize = sizeof(int);
const size_t kInt8Size = sizeof(int8_t);
const size_t kInt16Size = sizeof(int16_t);
const size_t kInt32Size = sizeof(int32_t);
const size_t kInt64Size = sizeof(int64_t);
const size_t kFloatSize = sizeof(float);
const size_t kDoubleSize = sizeof(double);
const size_t kMetaSize = sizeof(MetaColumn);

struct ColumnInfo {
  ColumnInfo() {
    column_size_ = 0;
    column_offset_ = 0;
  }
  ColumnInfo(const char* column_name, const ValueType& column_type) {
    Init(column_name, column_type);
  }
  ColumnInfo(const char* column_name, const ValueType& column_type,
             const size_t& column_size) {
    Init(column_name, column_type, column_size);
  }
  ColumnInfo(const ColumnInfo& column_info) {
    Copy(column_info);
  }

  void Copy(const ColumnInfo& column_info) {
    memcpy(column_name_, column_info.column_name_, 16);
    column_type_ = column_info.column_type_;
    column_size_ = column_info.column_size_;
    column_offset_ = column_info.column_offset_;
  }

  void Init(const char* column_name, const ValueType &column_type,
            const size_t& column_size = 0) {
    assert(strlen(column_name) < 16);
    size_t len = strlen(column_name);
    memcpy(column_name_, column_name, len);
    column_name_[len] = '\0';
    column_type_ = column_type;
    if (column_type != ValueType::VARCHAR) {
      switch (column_type) {
        case INT:
          column_size_ = kIntSize;
          break;
        case INT8:
          column_size_ = kInt8Size;
          break;
        case INT16:
          column_size_ = kInt16Size;
          break;
        case INT32:
          column_size_ = kInt32Size;
          break;
        case INT64:
          column_size_ = kInt64Size;
          break;
        case DOUBLE:
          column_size_ = kDoubleSize;
          break;
        case FLOAT:
          column_size_ = kFloatSize;
          break;
        case META:
          column_size_ = kMetaSize;
        default:
          break;
      }
    } else {
      column_size_ = column_size;
      assert(column_size_ > 0);
    }

    column_offset_ = 0;
  }

  char column_name_[16];
  ValueType column_type_;
  size_t column_size_;
  // used in RecordSchema
  size_t column_offset_;
};
}

#endif
