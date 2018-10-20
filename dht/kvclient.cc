// Copyright (c) 2018 The GAM Authors 

#include "kvclient.h"
#include "kv.h"
#include "chars.h"
#include "gallocator.h"

namespace dht{

kvClient::kvClient(GAlloc* alloc) : allocator_(alloc) {

  char a[1024];

  memset(a, 0, 1024);

  allocator_->HTable((void*)a);

  GAddr addr;

  char* p = a;

  p += readInteger(p, addr);
  while (addr != Gnullptr){
    htables_.push_back(addr);
    p += readInteger(p, addr);
  } 

  if (p - a == sizeof(addr)) {
    epicPanic("Cannot find any hash table instances");
  } else {
    std::sort(htables_.begin(), htables_.end());
    epicLog(LOG_WARNING, "%d hash tables received", htables_.size());
  }
}

GAddr kvClient::getBktAddr(hash_t key) {

  uint16_t wid = TBL(key) % this->htables_.size();

  return htables_[wid] + (BKT_SIZE * BUCKET(key));
}

int kvClient::put(char* key, char* value){

  kv* kvp = new kv(strlen(key), strlen(value), key, value, kv_);

  int pos;
  uint32_t tag, size;
  GAddr addr;
  char* p = this->bkt_;

  GAddr bktAddr = this->getBktAddr(kvp->hkey);
  while(1) {
    // read bucket
    allocator_->WLock(bktAddr, BKT_SIZE);
    if (allocator_->Read(bktAddr, this->bkt_, BKT_SIZE) != BKT_SIZE) {
      allocator_->UnLock(bktAddr, BKT_SIZE);
      return -1;
    }

    for (pos = 0; pos < BKT_SLOT; ++pos)
    {
      p = bkt_ + pos * ENTRY_SIZE;
      if (matchTag(p, TAG(kvp->hkey)) || !containTag(p))
        break;
    }

    if (pos < BKT_SLOT) break;

    GAddr tmp;
    readInteger(bkt_ + ENTRY_SIZE * BKT_SLOT, tmp);
    if (tmp == 0) {
      //allocator_->WLock(bktAddr, BKT_SIZE);
      tmp = allocator_->Malloc(BKT_SIZE);
      appendInteger(bkt_ + ENTRY_SIZE * BKT_SLOT, tmp);
      allocator_->Write(bktAddr, bkt_, BKT_SIZE);
    }

    allocator_->UnLock(bktAddr, BKT_SIZE);
    bktAddr = tmp;
  } 

  epicAssert(pos < BKT_SLOT);

  parseEntry(p, tag, size, addr);
  //allocator_->WLock(bktAddr, BKT_SIZE);
  if (size < kvp->size()) {
    if (tag > 0) {
      epicAssert(addr > 0);
      this->allocator_->Free(addr);
    } else {
      epicAssert(size == 0);
    }
    addr = this->allocator_->Malloc(kvp->size());
  }

  if (size != kvp->size()) {
    size = kvp->size();
    tag = TAG(kvp->hkey);
    updateEntry(p, tag, size, addr);
    allocator_->Write(bktAddr, bkt_, BKT_SIZE);
    //fprintf(stdout, "bktAddr = %lx, key = %lx, bucket = %lx, tag = %x, size = %d, addr = %lx, p = %lx, bkt = %lx\n", bktAddr, kvp->hkey, BUCKET(kvp->hkey), tag, size, addr, p, bkt_);
  }

  int ret = allocator_->Write(addr, (void*)(kvp->base()), kvp->size());
  allocator_->UnLock(bktAddr, BKT_SIZE);

  return ret == 0 ? -1 : ret;
}

int kvClient::get(hash_t key, kv** kv) {

  GAddr bktAddr = this->getBktAddr(key);
  int pos;
  uint32_t tag, size;
  GAddr addr;
  char* p = nullptr;

  //fprintf(stdout, "bktAddr = %lx, key = %lx, bucket = %lx, tag = %lx\n", bktAddr, key, BUCKET(key), TAG(key));

  while(1) {
    // read bucket
    allocator_->RLock(bktAddr, BKT_SIZE);
    if (allocator_->Read(bktAddr, this->bkt_, BKT_SIZE) != BKT_SIZE) {
      allocator_->UnLock(bktAddr, BKT_SIZE);
      return -1;
    }

    for (pos = 0; pos < BKT_SLOT; ++pos)
    {
      p = bkt_ + pos * ENTRY_SIZE;
      if (matchTag(p, TAG(key)))
        break;
    }

    if (pos < BKT_SLOT)
      break;

    GAddr tmp;
    readInteger(bkt_ + ENTRY_SIZE * BKT_SLOT, tmp);
    if (tmp == 0)
      break;

    allocator_->UnLock(bktAddr, BKT_SIZE);
    bktAddr = tmp;
  }

  if (pos == BKT_SLOT) {
    allocator_->UnLock(bktAddr, BKT_SIZE);
    return -1; //not found
  }
  parseEntry(p, tag, size, addr);
  int ret = allocator_->Read(addr, this->kv_, size);
  allocator_->UnLock(bktAddr, BKT_SIZE);
  *kv = new class kv(kv_);
  return ret == 0 ? -1 : ret;
}

int kvClient::del(hash_t key) {

  GAddr bktAddr = this->getBktAddr(key);
  int pos;
  uint32_t tag, size;
  GAddr addr;
  char* p = nullptr;

  while(1) {
    // read bucket
    allocator_->WLock(bktAddr, BKT_SIZE);
    if (allocator_->Read(bktAddr, this->bkt_, BKT_SIZE) != BKT_SIZE) {
      allocator_->UnLock(bktAddr, BKT_SIZE);
      return -1;
    }

    for (pos = 0; pos < BKT_SLOT; ++pos)
    {
      p = bkt_ + pos * ENTRY_SIZE;
      if (matchTag(p, TAG(key)))
        break;
    }

    if (pos < BKT_SIZE)
      break;

    GAddr tmp;
    readInteger(bkt_ + ENTRY_SIZE * BKT_SLOT, tmp);
    if (tmp == 0)
      break;

    allocator_->UnLock(bktAddr, BKT_SIZE);
    bktAddr = tmp;
  }

  if (pos == BKT_SIZE) return -1; //not found
  parseEntry(p, tag, size, addr);
  //allocator_->WLock(bktAddr, BKT_SIZE);
  allocator_->Free(addr);
  updateEntry(p, 0, 0, 0);
  allocator_->Write(bktAddr, this->bkt_, BKT_SIZE);
  allocator_->UnLock(bktAddr, BKT_SIZE);

  return 0;
}
};
