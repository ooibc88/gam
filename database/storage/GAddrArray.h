#ifndef __DATABASE_STORAGE_GADDR_ARRAY_H__
#define __DATABASE_STORAGE_GADDR_ARRAY_H__

#include "GAMObject.h"

namespace Database {
template<class T>
class GAddrArray : public GAMObject{
 public:
  GAddrArray() {
  }
  ~GAddrArray() {
  }

  void Init(const uint64_t& element_count, 
      size_t element_size, GAlloc* gallocator) {
    // Decide the size for each layer
    array_size_ = element_count;
    element_size_ = element_size;
    uint64_t bound = 1;
    layer_num_ = 0;
    while (bound < array_size_) {
      bound *= N;
      layer_num_++;
    }
    layer_size_[0] = 1;
    for (int i = 1; i <= layer_num_; ++i) {
      layer_size_[i] = layer_size_[i - 1] * N;
    }
    assert(layer_num_ < 4);
    assert(array_size_ <= layer_size_[layer_num_]);

    // Allocate memory for each layer
    if (layer_num_ == 1) {
      root_addr_ = gallocator->AlignedMalloc(element_size * N);
    } else {
      // The last layer is of T, while others are of GAddr
      root_addr_ = gallocator->AlignedMalloc(sizeof(GAddr) * N);
      for (int layer_id = 1; layer_id < layer_num_; ++layer_id) {
        for (uint64_t pos = 0; pos < layer_size_[layer_id]; ++pos) {
          size_t per_slot_size = layer_id < layer_num_ - 1 ? 
            sizeof(GAddr) : element_size;
          GAddr mem_segment = gallocator->AlignedMalloc(per_slot_size * N);
          GAddr parent_addr = LocateParentNode(layer_id, pos, gallocator);
          // index of this subarray: pos % N
          gallocator->Write(parent_addr, (pos % N) * sizeof(GAddr),
                            &mem_segment, sizeof(GAddr));
        }
      }
    }
  }

  GAddr GetElementGAddr(uint64_t offset, GAlloc *gallocator) {
    assert(offset < array_size_);
    GAddr parent_addr = LocateParentNode(layer_num_, offset, gallocator);
    GAddr ret = GADD(parent_addr, (offset % N) * element_size_);
    return ret;
  }

  virtual void Serialize(const GAddr& addr, GAlloc *gallocator) {
    size_t off = 0;
    gallocator->Write(addr, off, &root_addr_, sizeof(GAddr));
    off += sizeof(GAddr);
    gallocator->Write(addr, off, &array_size_, sizeof(uint64_t));
    off += sizeof(uint64_t);
    gallocator->Write(addr, off, &element_size_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Write(addr, off, layer_size_, sizeof(uint64_t)*4);
    off += sizeof(uint64_t)*4;
    gallocator->Write(addr, off, &layer_num_, sizeof(size_t));
  }
  
  virtual void Deserialize(const GAddr& addr, GAlloc *gallocator) {
    size_t off = 0;
    gallocator->Read(addr, off, &root_addr_, sizeof(GAddr));
    off += sizeof(GAddr);
    gallocator->Read(addr, off, &array_size_, sizeof(uint64_t));
    off += sizeof(uint64_t);
    gallocator->Read(addr, off, &element_size_, sizeof(size_t));
    off += sizeof(size_t);
    gallocator->Read(addr, off, layer_size_, sizeof(uint64_t)*4);
    off += sizeof(uint64_t)*4;
    gallocator->Read(addr, off, &layer_num_, sizeof(size_t));
  }
    
  static size_t GetSerializeSize() {
    return sizeof(GAddr) + sizeof(uint64_t)*5 + sizeof(size_t)*2;
  }

private:
  GAddrArray(const GAddrArray&);
  GAddrArray& operator=(const GAddrArray&);

  GAddr LocateParentNode(int layer_id, uint64_t pos, GAlloc *gallocator) {
    // in order to go to 'layer_id'th layer, 
    // 'pos'th entry of this layer, 
    // need to locate parent node which contains the array address
    // No. of elements in ith layer = N^i
    assert(pos < layer_size_[layer_id]);
    uint64_t path[layer_num_ + 1];
    for (int j = layer_id - 1; j >= 0; --j) {
      pos = pos / N;
      // now, pos is the 'pos'th entry in jth layer
      path[j] = pos % N;
      // path[j] is the location of the subarray
    }
    GAddr cur_addr = root_addr_;
    for (int j = 1; j <= layer_id - 1; ++j) {
      GAddr tmp = 0;
      gallocator->Read(cur_addr, path[j] * sizeof(GAddr), &tmp, sizeof(GAddr));
      cur_addr = tmp;
    }
    return cur_addr;
  }

private:
  GAddr root_addr_;
  uint64_t array_size_;
  size_t element_size_;

  uint64_t layer_size_[4];
  size_t layer_num_;

  const static int N = 1024;
};
}

#endif
