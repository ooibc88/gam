// Copyright (c) 2018 The GAM Authors 

#include "farm.h"
#include "farm_txn.h"
#include "log.h"
#include "zmalloc.h"
#include "kernel.h"

#include <cstring>

using std::vector;
using std::unique_ptr;
using std::string;
using std::unordered_map;

#define vstring std::vector<std::string>

Farm::Farm(Worker* w): w_(w), tx_(nullptr), wh_(new WorkerHandle(w)), rtx_(new TxnContext()) {}

int Farm::txBegin() {
  if (unlikely(tx_ != nullptr)) {
    epicLog(LOG_INFO, "There is already a transaction running; You may want to call txCommit before start a new one!!!");
    return -1;
  }

  tx_ = rtx_.get();
  tx_->reset();
  return 0;
}

GAddr Farm::txAlloc(size_t size, GAddr addr){
  if (unlikely(tx_ == nullptr)) {
    epicLog(LOG_FATAL, "Call txBegin first before any transactional allocation/read/write/free");
    return Gnullptr;
  }

  tx_->wr_->op = FARM_MALLOC;
  tx_->wr_->addr = addr;

  // each object is associated with version and size
  tx_->wr_->size = size + sizeof(version_t) + sizeof(osize_t);

  wh_->SendRequest(tx_->wr_);

  if (tx_->wr_->status == SUCCESS) {
    addr = tx_->wr_->addr;

    // mark this object as to free so that it will get released provided
    // not get written
    Object* o = tx_->createWritableObject(addr);

    // initiate this object as zero-byte
    o->setVersion(0);
    o->setSize(0);
    //txWrite(addr, nullptr, 0);
    return tx_->wr_->addr;
  }
  else
    return Gnullptr;
}

void Farm::txFree(GAddr addr) {
  if (tx_ == nullptr) {
    epicLog(LOG_FATAL, "Call txBegin first before any transactional allocation/read/write/free");
    return;
  }

  // mark it as "to be freed" by setting size to -1
  txWrite(addr, nullptr, -1);
}

osize_t Farm::txRead(GAddr addr, char* buf, osize_t size) {
  if (unlikely(tx_ == nullptr)) {
    epicLog(LOG_FATAL, "Call txBegin first before any transactional allocation/read/write/free");
    return -1;
  }

  Object* o = tx_->getReadableObject(addr);
  // check if there is already a readable copy
  if (o != nullptr) {
    goto success;
  }

  if (this->w_->IsLocal(addr)) {
    // process locally
    void* local = w_->ToLocal(addr);

    // lock-free read
    version_t before, after;
    o = tx_->createReadableObject(addr);
    //o->deserialize((const char*)local);
    after = __atomic_load_n((version_t*)local, __ATOMIC_ACQUIRE);
    do {
      before = after;
      while(is_version_wlocked(before))
        before = __atomic_load_n((version_t*)local, __ATOMIC_ACQUIRE);
      runlock_version(&before);
      char* t = (char*)local + sizeof(before);
      osize_t s;
      t += readInteger(t, s);
      o->setVersion(before);
      o->setSize(s);
      o->readEmPlace(t, 0, s);
      after = __atomic_load_n((version_t*)local, __ATOMIC_ACQUIRE);
    } while (is_version_diff(before, after));


    if (o->getSize() == -1 || o->getVersion() == 0) {
      // version == 0 means this object has not been written after being
      // allocated
      tx_->rmReadableObject(addr);
      goto fail;
    }
    goto success;
  }

  tx_->wr_->op = FARM_READ;
  tx_->wr_->addr = addr;

  if (wh_->SendRequest(tx_->wr_) != SUCCESS)  {
    goto fail;
  }

  o = tx_->getReadableObject(addr);

success:
  if (o && buf && o->getSize() > 0 && size >= o->getSize()) {
    return o->writeTo(buf);
  }

fail:
  return 0;
}

osize_t Farm::txPartialRead(GAddr addr, osize_t offset, char* buf, osize_t size) {
  if (unlikely(tx_ == nullptr)) {
    epicLog(LOG_FATAL, "Call txBegin first before any transactional allocation/read/write/free");
    return -1;
  }

  Object* o = tx_->getReadableObject(addr);

  if (o == nullptr) {
    // first read the whole object
    txRead(addr, nullptr, 0);
    o = tx_->getReadableObject(addr);
  }

  if (likely(o)) {
    return o->writeTo(buf, offset, size);
  }

  return 0;
}

osize_t Farm::txPartialWrite(GAddr addr, osize_t offset, const char* buf, osize_t size) {
  if (unlikely(tx_ == nullptr)) {
    epicLog(LOG_FATAL, "Call txBegin first before any transactional allocation/read/write/free");
    return -1;
  }

  bool blind = true;
  if (tx_->getReadableObject(addr) || tx_->getWritableObject(addr))
    blind = false;

  if (blind) {
    txRead(addr, nullptr, 0);
    //tx_->rmReadableObject(addr);
  }

  Object* o = tx_->createWritableObject(addr);
  return o->readEmPlace(buf, offset, size);
}

osize_t Farm::txWrite(GAddr addr, const char* buf, osize_t size) {
  if (unlikely(tx_ == nullptr)) {
    epicLog(LOG_FATAL, "Call txBegin first before any transactional allocation/read/write/free");
    return -1;
  }

  Object* o = tx_->createWritableObject(addr);
  if (size < 0 || o->getSize() == -1) {
    // free this object
    o->setSize(-1);
    return 0;
  }
  else
    o->setSize(o->readEmPlace(buf, 0, size));
  return size;
}

int Farm::txCommit() {
  if (unlikely(tx_ == nullptr)) {
    epicLog(LOG_FATAL, "There is no active transaction!!!!!!!");
    return -1;
  }

  if (txnIsLocal()){
    tx_->wr_->op = Work::FARM_READ; // a trick to indicate this is an app commit
    this->w_->FarmProcessLocalCommit(tx_->wr_);
    bool ret = (tx_->wr_->status == Status::SUCCESS) ? 0 : -1;
    tx_ = nullptr;
    return ret;
  }

  //tx_->updateVesion();
  tx_->wr_->op = COMMIT;
  int ret = wh_->SendRequest(tx_->wr_);
  tx_ = nullptr;
  return ret != 0 ? -1 : ret;
}

int Farm::txAbort() {
  if (unlikely(tx_ == nullptr)) {
    epicLog(LOG_FATAL, "There is no active transaction!!!!!!!");
    return -1;
  }

  tx_->reset();
  tx_ = nullptr;
  return 0;
}

int Farm::put(uint64_t key, const void* value, size_t count) {
  this->txBegin();
  WorkRequest* wr = this->tx_->wr_;
  wr->size = count;
  wr->key = key;
  wr->ptr = const_cast<void*>(value);
  wr->op = PUT;

  int ret = -1;

  if (this->wh_->SendRequest(wr)) {
    epicLog(LOG_WARNING, "Put failed");
  } else {
    ret = count;
  }

  this->txCommit();
  return ret;
}

int Farm::get(uint64_t key, void* value) {
  this->txBegin();
  WorkRequest* wr = this->tx_->wr_;
  wr->key = key;
  wr->op = GET;
  wr->ptr = value;
  int ret = -1;

  if (wh_->SendRequest(wr)) {
    epicLog(LOG_WARNING, "Get failed");
  } else {
    ret = wr->size;
    //memcpy(value, wr->ptr, wr->size);
  }

  this->txCommit();
  return ret;
}

int Farm::kv_put(uint64_t key, const void* value, size_t count, int node_id) {
  bool newtx = false;
  if(likely(tx_ == nullptr)) newtx = true;
  if(newtx) this->txBegin();
  WorkRequest* wr = this->tx_->wr_;
  wr->size = count;
  wr->key = key;
  wr->ptr = const_cast<void*>(value);
  wr->op = KV_PUT;
  wr->counter = node_id;

  int ret = -1;

  if (this->wh_->SendRequest(wr)) {
    epicLog(LOG_INFO, "Put failed");
  } else {
    ret = count;
  }

  if(newtx) this->txCommit();
  return ret;
}

int Farm::kv_get(uint64_t key, void* value, int node_id) {
  bool newtx = false;
  if(likely(tx_ == nullptr)) newtx = true;
  if(newtx) this->txBegin();
  WorkRequest* wr = this->tx_->wr_;
  wr->key = key;
  wr->op = KV_GET;
  wr->ptr = value;
  wr->counter = node_id;
  int ret = -1;

  if (wh_->SendRequest(wr)) {
    epicLog(LOG_INFO, "Get failed");
  } else {
    ret = wr->size;
    //memcpy(value, wr->ptr, wr->size);
  }

  if(newtx) this->txCommit();
  return ret;
}
