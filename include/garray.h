// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_GARRAY_H_
#define INCLUDE_GARRAY_H_
#include "gallocator.h"
#include "slabs.h"

template<typename T, size_t BUCKET_SIZE = ITEM_SIZE_MAX>
class GArray {
  GAddr root_addr_ = Gnullptr;
  size_t size_ = 0;
  size_t capacity_ = 0;
  int layer_ = 0;
  Flag flag_ = 0;

  const int header_size = 32;  //GAddr+size_t+size_t+int*2
  const int items_per_bucket = BUCKET_SIZE / sizeof(T);
  const int gaddrs_per_bucket = BUCKET_SIZE / sizeof(GAddr);
  GAlloc* galloc_;
  GAddr header_ = Gnullptr;  //the global address storing this class information

  GAddr Locate(size_t off, GAlloc* galloc) {
    epicAssert(root_addr_);
    //epicAssert(off < size_);
    int offs[layer_];
    offs[layer_ - 1] = off % items_per_bucket;
    size_t tmp = off / items_per_bucket;
    for (int i = layer_ - 2; i >= 0; i--) {
      offs[i] = tmp % gaddrs_per_bucket;
      tmp /= gaddrs_per_bucket;
    }
    epicAssert(tmp == 0);

    GAddr curr_addr = root_addr_;
    for (int i = 0; i < layer_ - 1; i++) {
      galloc->Read(curr_addr, sizeof(GAddr) * offs[i], &curr_addr,
                   sizeof(GAddr));
    }
    return GADD(curr_addr, offs[layer_ - 1] * sizeof(T));
  }

  /*
   * locate the pointer address in layer "layer" that points to the "idx"th block in layer "layer+1"
   */
  GAddr LocateInternal(GAddr root_addr, int total_layer, int layer, size_t idx,
                       GAlloc* galloc) {
    epicAssert(root_addr);
    int offs[layer];
    size_t bks = 0;
    int num_per_bucket = 0;
    int item_size = 0;
    if (layer == total_layer) {
      num_per_bucket = items_per_bucket;
      item_size = sizeof(T);
    } else {
      num_per_bucket = gaddrs_per_bucket;
      item_size = sizeof(GAddr);
    }
    offs[layer - 1] = idx % num_per_bucket;
    bks = idx / num_per_bucket;
    for (int i = layer - 2; i >= 0; i--) {
      offs[i] = bks % gaddrs_per_bucket;
      bks /= gaddrs_per_bucket;
    }
    epicAssert(bks == 0);

    GAddr curr_addr = root_addr;
    for (int i = 0; i < layer - 1; i++) {
      galloc->Read(curr_addr, sizeof(GAddr) * offs[i], &curr_addr,
                   sizeof(GAddr));
    }
    return GADD(curr_addr, offs[layer - 1] * item_size);
  }

  void SyncFrom(GAlloc* galloc) {
    //TODO: change to optimistic strategy
    if (header_) {  //update the header in case others resize the array (pessimistic)
      galloc->Read(header_, this, header_size);
    }
  }

  void SyncTo(GAlloc* galloc) {
    if (header_) {  //update the header in case others resize the array (pessimistic)
      galloc->Write(header_, this, header_size);
    }
  }

  int Layers(size_t size) {
    return ceil(
        log((double) size / items_per_bucket) / log((double) gaddrs_per_bucket))
        + 1;
  }

  size_t Capacity(size_t size) {
    return ceil_divide(size, items_per_bucket) * items_per_bucket;
  }

  size_t CalLayers(size_t size, long layers[], int layer) {
    layers[layer - 1] = ceil_divide(size, items_per_bucket);
    size_t capacity = layers[layer - 1] * items_per_bucket;
    for (int i = layer - 2; i >= 0; i--) {
      layers[i] = ceil_divide(layers[i + 1], gaddrs_per_bucket);
    }
    epicAssert(layers[0] == 1);
    return capacity;
  }

  /*
   * insert a new block indexed at idx in layer l+1 to corresponding layer l
   */
  inline void InsertBlock(GAddr root_addr, int total_layer, int l, size_t idx,
                          GAddr newbk, GAlloc* galloc) {
    GAddr addr = LocateInternal(root_addr, total_layer, l, idx, galloc);  //parent layer storing the pointer to next layer
    galloc->Write(addr, &newbk, sizeof(GAddr));
  }

  void LockHeader(GAlloc* galloc) {
    epicAssert(header_);
    galloc->WLock(header_, header_size);
  }

  void UnLockHeader(GAlloc* galloc) {
    epicAssert(header_);
    galloc->UnLock(header_, header_size);
  }

  GAddr Globalize(GAlloc* galloc) {
    epicAssert(!header_);
    header_ = galloc->AlignedMalloc(header_size, flag_);
    galloc->Write(header_, this, header_size);
    return header_;
  }

 public:
  GArray(GAlloc* galloc, Flag flag = 0)
      : galloc_(galloc),
        flag_(flag) {
  }
  ;
  GArray(const GArray& clone, GAlloc* galloc)
      : galloc_(galloc) {
    Clone(clone.Header(), galloc);
  }
  GArray(size_t size, GAlloc* galloc, Flag flag = 0)
      : size_(size),
        galloc_(galloc),
        flag_(flag) {
    if (size <= items_per_bucket) {
      root_addr_ = galloc->AlignedMalloc(BUCKET_SIZE, flag_);
      layer_ = 1;
      capacity_ = items_per_bucket;
    } else {
      layer_ = Layers(size);
      if (layer_ >= 2) {
        epicLog(LOG_WARNING, "array layer larger than 2 (array layer = %d)",
                layer_);
      }
      long layers_no[layer_];
      capacity_ = CalLayers(size, layers_no, layer_);

      root_addr_ = galloc->AlignedMalloc(BUCKET_SIZE, flag_);  //layer 1
      for (int i = 1; i < layer_; i++) {  //layer 2 to layer_
        for (int j = 0; j < layers_no[i]; j++) {
          GAddr newbk = galloc->AlignedMalloc(BUCKET_SIZE, flag_);
          InsertBlock(root_addr_, layer_, i, j, newbk, galloc);
        }
      }
    }
    Globalize(galloc);
  }

  /*
   * Must be called after Globalize cos we have to lock in case there are multiple resize operations
   */
  void Resize(size_t n, GAlloc* galloc = nullptr) {
    if (!galloc)
      galloc = galloc_;
    epicAssert(galloc);
    LockHeader(galloc);
    SyncFrom(galloc);
    if (n > size_) {  //enlarge: 1. allocate memory; 2. update header; 3. sync
      if (n > capacity_) {  //if n <= capacity_, nothing to do except update the size_ member
        epicLog(LOG_WARNING, "have to resize from %lu to %lu", size_, n);
        int layer = Layers(n);
        long old_layers[layer_];
        long new_layers[layer];
        CalLayers(size_, old_layers, layer_);
        capacity_ = CalLayers(n, new_layers, layer);
        if (layer > layer_) {
          GAddr old_root = root_addr_;
          int old_layer = layer_;
          layer_ = layer;
          root_addr_ = galloc->AlignedMalloc(BUCKET_SIZE, flag_);
          epicLog(LOG_WARNING, "increased layer from %d to %d", old_layer,
                  layer);

          //1. generate the new layers higher than previous layers
          int i = 1;
          for (i = 1; i < layer - old_layer; i++) {
            for (long j = 0; j < new_layers[i]; j++) {
              GAddr newbk = galloc->AlignedMalloc(BUCKET_SIZE, flag_);
              InsertBlock(root_addr_, layer_, i, j, newbk, galloc);
            }
          }

          //2. put the old root layer to the new layer
          InsertBlock(root_addr_, layer_, i, 0, old_root, galloc);
          for (long j = 1; j < new_layers[i]; j++) {
            GAddr newbk = galloc->AlignedMalloc(BUCKET_SIZE, flag_);
            InsertBlock(root_addr_, layer_, i, j, newbk, galloc);
          }
          i++;

          //extend the old layers below old root layer
          for (; i < layer; i++) {
            for (long j = old_layers[i - (layer - old_layer)];
                j < new_layers[i]; j++) {
              GAddr newbk = galloc->AlignedMalloc(BUCKET_SIZE, flag_);
              InsertBlock(root_addr_, layer_, i, j, newbk, galloc);
            }
          }
        } else {
          epicAssert(layer == layer_);
          //extend the old layers below old root layer
          for (int i = 1; i < layer; i++) {
            for (long j = old_layers[i]; j < new_layers[i]; j++) {
              GAddr newbk = galloc->AlignedMalloc(BUCKET_SIZE, flag_);
              InsertBlock(root_addr_, layer_, i, j, newbk, galloc);
            }
          }
        }
      }
      size_ = n;
      SyncTo(galloc);
    } else if (n < size_) {  //shrink: 1. update header; 2. sync; 3. free memory
      epicLog(LOG_WARNING, "shrink from %lu to %lu", size_, n);
      long old_size = size_;
      int old_layer = layer_;
      GAddr old_root = root_addr_;
      size_ = n;
      layer_ = Layers(n);
      capacity_ = Capacity(n);

      //locate the new root block
      GAddr curr_addr = old_root;
      for (int i = 0; i < old_layer - layer_; i++) {
        galloc->Read(curr_addr, &curr_addr, sizeof(GAddr));
      }
      root_addr_ = curr_addr;

      //sync first in case other nodes tried to access deleted blocks
      SyncTo(galloc);

      //wait for enough time before an in-process array read/write
      //TODO: add re-do check if the process time exceed this time
      usleep(old_layer * MAX_RW_TIME);

      long old_layers[old_layer];
      long new_layers[old_layer];
      int reduced_layer = old_layer - layer_;
      for (int i = 0; i < reduced_layer; i++) {
        new_layers[i] = 0;
      }
      CalLayers(old_size, old_layers, old_layer);
      CalLayers(n, &new_layers[reduced_layer], layer_);

      //start release the memory bottom-up
      for (int i = old_layer - 1; i > 0; i--) {
        for (int j = old_layers[i] - 1; j >= new_layers[i]; j--) {
          GAddr parent = LocateInternal(old_root, old_layer, i, j, galloc);
          GAddr blk;
          galloc->Read(parent, &blk, sizeof(GAddr));
          galloc->Free(blk);
        }
      }
      if (reduced_layer) {
        galloc->Free(old_root);
      }
    } else {
      epicLog(
          LOG_WARNING,
          "There may be a race here, since the targeted resize size (%lu) equals the original size (%lu)",
          n, size_);
    }
    epicLog(LOG_WARNING, "end resize");
    UnLockHeader(galloc);
  }

  inline void Clone(GAddr header, GAlloc* galloc = nullptr) {
    if (!galloc)
      galloc = galloc_;
    epicAssert(galloc);
    epicAssert(!header_);
    header_ = header;
    SyncFrom(galloc);
  }

  inline bool IsGlobal() {
    return header_ ? true : false;
  }

  inline int Read(size_t idx, T* val, GAlloc* galloc = nullptr) {
    if (!galloc)
      galloc = galloc_;
    epicAssert(galloc);
    SyncFrom(galloc);
    epicAssert(idx < size_);
    GAddr addr = Locate(idx, galloc);
    return galloc->Read(addr, val, sizeof(T));
  }

  inline int Write(size_t idx, T* val, GAlloc* galloc = nullptr) {
    if (!galloc)
      galloc = galloc_;
    epicAssert(galloc);
    SyncFrom(galloc);
    epicAssert(idx < size_);
    return galloc->Write(Locate(idx, galloc), val, sizeof(T));
  }

  inline int Layers() const {
    return layer_;
  }

  inline size_t Size() const {
    return size_;
  }

  inline size_t Capacity() const {
    return capacity_;
  }

  inline GAddr Header() const {
    return header_;
  }

  bool operator ==(GArray<T, BUCKET_SIZE>& ga) {
    return Equal(ga);
  }

  bool Equal(GArray<T, BUCKET_SIZE>& ga, GAlloc* galloc = nullptr) {
    if (!galloc)
      galloc = galloc_;
    epicAssert(galloc);
    if (size_ != ga.Size()) {
      return false;
    }
    for (size_t i = 0; i < size_; i++) {
      T val1, val2;
      Read(i, &val1, galloc);
      ga.Read(i, &val2, galloc);
      if (val1 != val2) {
        return false;
      }
    }
    return true;
  }

};

#endif /* INCLUDE_GARRAY_H_ */
