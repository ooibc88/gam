#ifndef __DATABASE_STORAGE_HASH_INDEX_HELPER_H__
#define __DATABASE_STORAGE_HASH_INDEX_HELPER_H__

#include <cassert>
#include <fstream>

#include "Record.h"
#include "Meta.h"

#define SLOT_NUM 2

namespace Database {
struct Items {
  void Init() {
    record_ptrs_count_ = 0;
    next_ = Gnullptr;
  }

  bool AddRecordPtr(const GAddr& record_ptr) {
    assert(record_ptrs_count_ <= SLOT_NUM);
    if (record_ptrs_count_ == SLOT_NUM) {
      return false; 
    }
    assert(record_ptrs_count_ < SLOT_NUM);
    record_ptrs_[record_ptrs_count_++] = record_ptr;
    return true;
  }

  GAddr record_ptrs_[SLOT_NUM];
  size_t record_ptrs_count_;
  GAddr next_; // address to the next Items
};

struct ItemsIter {
  ItemsIter(const GAddr &items_addr) :
    items_addr_(items_addr), pos_(-1) {

  }

  GAddr GetNext(GAlloc *gallocator) {
    if (items_addr_ == Gnullptr) {
      return Gnullptr;
    }
    if (pos_ < 0) {
      gallocator->Read(items_addr_, &items_, sizeof(Items));
      pos_ = 0;
    }
    assert(pos_ < items_.record_ptrs_count_);
    GAddr ret = items_.record_ptrs_[pos_++];
    if (pos_ == items_.record_ptrs_count_) {
      items_addr_ = items_.next_;
      pos_ = -1;
    }
    return ret;
  }

  GAddr items_addr_;
  int pos_;
  Items items_;
};

struct BucketNode {
  void Init(const IndexKey& k = 0) {
    key_ = k;
    next_ = Gnullptr;
    items_list_ = Gnullptr;
  }

  IndexKey key_;
  // address for next bucket node (with different keys)
  GAddr next_;
  // address for the first Items
  // which tracks a list of records of the same key
  GAddr items_list_;
};

class BucketHeader : public GAMObject{
 public:
  BucketHeader() {
  }
  ~BucketHeader() {
  }

  void Init() {
    head_ = Gnullptr;
  }

  // return false if the key exists
  bool InsertNode(const IndexKey& key, const GAddr& record_ptr,
                  GAlloc *gallocator) {
    GAddr cur_node_addr = head_;
    GAddr prev_node_addr = Gnullptr;
    BucketNode cur_node;
    while (cur_node_addr != Gnullptr) {
      gallocator->Read(cur_node_addr, &cur_node, sizeof(BucketNode));
      if (cur_node.key_ == key) {
        break;
      }
      prev_node_addr = cur_node_addr;
      cur_node_addr = cur_node.next_;
    }
    if (cur_node_addr != Gnullptr) { // There exists a match for key 
      Items items;
      gallocator->Read(cur_node.items_list_, &items, sizeof(Items));
      if (items.AddRecordPtr(record_ptr)) { // success
        gallocator->Write(cur_node.items_list_, &items, sizeof(Items));
      } else { // no available slots
        // write to new Items
        Items new_items;
        new_items.Init();
        new_items.AddRecordPtr(record_ptr);
        new_items.next_ = cur_node.items_list_;
        GAddr new_items_addr = gallocator->AlignedMalloc(sizeof(Items));
        gallocator->Write(new_items_addr, &new_items, sizeof(new_items));
        // update the head of linked list
        cur_node.items_list_ = new_items_addr;
        gallocator->Write(cur_node_addr, &cur_node, sizeof(BucketNode));
      }
      return false;
    } else { // no match for key
      Items new_items;
      new_items.Init();
      new_items.AddRecordPtr(record_ptr);
      GAddr new_items_addr = gallocator->AlignedMalloc(sizeof(Items));
      gallocator->Write(new_items_addr, &new_items, sizeof(Items));

      BucketNode new_node;
      new_node.Init(key);
      new_node.items_list_ = new_items_addr;
      GAddr new_node_addr = gallocator->AlignedMalloc(sizeof(BucketNode));
      gallocator->Write(new_node_addr, &new_node, sizeof(BucketNode));
      
      if (prev_node_addr != Gnullptr) {
        BucketNode prev_node;
        gallocator->Read(prev_node_addr, &prev_node,
                                     sizeof(BucketNode));
        assert(prev_node.next_ == Gnullptr);
        prev_node.next_ = new_node_addr;
        gallocator->Write(prev_node_addr, &prev_node, sizeof(BucketNode));
      } else { // no BucketNode in BucketHeader
        assert(head_ == Gnullptr);
        head_ = new_node_addr;
      }
      return true;
    }
  }

  bool SearchNode(const IndexKey& key, GAddr& items_list,
                  GAlloc* gallocator) const {
    GAddr cur_node_addr = head_;
    BucketNode cur_node;
    while (cur_node_addr != Gnullptr) {
      gallocator->Read(cur_node_addr, &cur_node, sizeof(BucketNode));
      if (cur_node.key_ == key) {
        break;
      }
      cur_node_addr = cur_node.next_;
    }
    if (cur_node_addr != Gnullptr) {
      items_list = cur_node.items_list_;
      return true;
    }
    return false;
  }

  virtual void Serialize(const GAddr& addr, GAlloc *gallocator) {
    gallocator->Write(addr, &head_, sizeof(GAddr));
  }
  virtual void Deserialize(const GAddr& addr, GAlloc *gallocator) {
    gallocator->Read(addr, &head_, sizeof(GAddr));
  }
  static size_t GetSerializeSize() {
    return sizeof(GAddr);
  }

private:
  GAddr head_;
};
}


#endif
