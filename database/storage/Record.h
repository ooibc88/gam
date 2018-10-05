// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_STORAGE_RECORD_H__
#define __DATABASE_STORAGE_RECORD_H__

#include "GAMObject.h"
#include "CharArray.h"
#include "Meta.h"
#include "RecordSchema.h"

namespace Database {
class Record : public GAMObject{
public:
  Record(RecordSchema *schema_ptr) : schema_ptr_(schema_ptr) {
    data_size_ = schema_ptr_->GetSchemaSize();
    data_ptr_ = new char[data_size_];
  }
  ~Record() {
    Release();
  }

  void SetColumn(size_t col_id, void *data) {
    size_t offset = schema_ptr_->GetColumnOffset(col_id);
    size_t col_size = schema_ptr_->GetColumnSize(col_id);
    memcpy(data_ptr_ + offset, data, col_size);
  }
  void SetColumn(size_t col_id, void *data, size_t data_size) {
    size_t offset = schema_ptr_->GetColumnOffset(col_id);
    memcpy(data_ptr_ + offset, data, data_size);
  }
  void GetColumn(size_t col_id, void *data) const {
    size_t offset = schema_ptr_->GetColumnOffset(col_id);
    size_t col_size = schema_ptr_->GetColumnSize(col_id);
    memcpy(data, data_ptr_ + offset, col_size);
  }

  bool GetVisible() const {
    size_t meta_col_id = schema_ptr_->GetMetaColumnId();
    MetaColumn meta_col;
    GetColumn(meta_col_id, &meta_col);
    return meta_col.is_visible_;
  }
  void SetVisible(bool val) {
    size_t meta_col_id = schema_ptr_->GetMetaColumnId();
    MetaColumn meta_col;
    this->GetColumn(meta_col_id, &meta_col);
    meta_col.is_visible_ = val;
    this->SetColumn(meta_col_id, &meta_col);
  }
  RecordSchema* GetSchema() {
    return schema_ptr_;
  }
  size_t GetSchemaSize() const {
    return schema_ptr_->GetSchemaSize();
  }
    

  virtual void Serialize(const GAddr& addr, GAlloc *gallocator) {
    gallocator->Write(addr, data_ptr_, data_size_);
  }
  void Serialize(const GAddr& addr, GAlloc *gallocator, size_t col_id) {
    size_t offset = schema_ptr_->GetColumnOffset(col_id);
    size_t col_size = schema_ptr_->GetColumnSize(col_id);
    gallocator->Write(addr, offset, data_ptr_ + offset, col_size);
  }

  virtual void Deserialize(const GAddr& addr, GAlloc *gallocator) {
    data_size_ = schema_ptr_->GetSchemaSize();
    if (!data_ptr_) {
      data_ptr_ = new char[data_size_];
    }
    gallocator->Read(addr, data_ptr_, data_size_);
  }

  size_t GetSerializeSize() const {
    return schema_ptr_->GetSchemaSize();
  }
  
private:
  Record(const Record&);
  Record& operator=(const Record&);

  void Release() {
    if (data_ptr_) {
      delete[] data_ptr_;
      data_ptr_ = nullptr;
    }
    data_size_ = 0;
  }

private:
  RecordSchema *schema_ptr_;
  char *data_ptr_;
  size_t data_size_;
};
}

#endif
