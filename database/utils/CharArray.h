// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_UTILS_CHAR_ARRAY_H__
#define __DATABASE_UTILS_CHAR_ARRAY_H__

#include <cstring>
#include <cstdint>

struct CharArray {
  CharArray()
      : size_(0),
        char_ptr_(NULL) {
  }
  size_t size_;
  char *char_ptr_;

  void HardCopy(const CharArray &char_array) {
    size_ = char_array.size_;
    char_ptr_ = new char[size_];
    memcpy(char_ptr_, char_array.char_ptr_, size_);
  }

  void SoftCopy(const CharArray &char_array) {
    size_ = char_array.size_;
    char_ptr_ = char_array.char_ptr_;
  }

  void Memcpy(const size_t &offset, const CharArray &char_array) const {
    memcpy(char_ptr_ + offset, char_array.char_ptr_, char_array.size_);
  }

  void Memcpy(const size_t &offset, const char *char_ptr,
              const size_t &size) const {
    memcpy(char_ptr_ + offset, char_ptr, size);
  }

  void Memset(const size_t &offset, const int &value,
              const size_t &size) const {
    memset(char_ptr_ + offset, value, size);
  }

  void Allocate(const size_t &size) {
    size_ = size;
    char_ptr_ = new char[size_];
    memset(char_ptr_, 0, size_);
  }

  void Release() {
    size_ = 0;
    if (char_ptr_ != NULL) {
      delete[] char_ptr_;
      char_ptr_ = NULL;
    }
  }

  void Clear() {
    size_ = 0;
    memset(char_ptr_, 0, size_);
  }
};

#endif
