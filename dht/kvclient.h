// Copyright (c) 2018 The GAM Authors 

#ifndef KV_CLIENT_H_
#define KV_CLIENT_H_

#include "kv.h"
#include <vector>

class GAlloc;

namespace dht{

struct kv{
  uint32_t klen;
  uint32_t vlen;
  char* key; 
  char* value;
  hash_t hkey;

  char* base(){
    return key - sizeof(klen) - sizeof(vlen);
  }

  uint32_t size() {
    return klen + vlen + sizeof(klen) + sizeof(vlen);
  }

  uint64_t hash() {
    return hkey;
  }

  kv() = delete;

  kv(uint32_t klen, uint32_t vlen, char* key, char* value, char* kv){
    this->klen = klen;
    this->vlen = vlen;
    char* p = kv;
    p += appendInteger(p, klen, vlen);
    this->key = p;
    this->value = p + klen;
    strncpy(this->key, key, klen);
    strncpy(this->value, value, vlen);
    hkey = std::stoull(std::string(key));
  }

  kv(char* kv) {
    kv += readInteger(kv, klen, vlen);
    epicAssert(klen >= 0 && vlen >= 0);
    key = kv;
    value = kv + klen;
  }
};

class kvClient{
  private:
    std::vector<GAddr> htables_;
    GAlloc* allocator_;
    int klen_;
    int vlen_;
    char bkt_[BKT_SIZE];
    char kv_[MAX_KV_SIZE];

    GAddr getBktAddr(hash_t);
  public:
    kvClient(GAlloc* alloc);
    int put(char*, char*);
    int get(hash_t, kv**);
    int del(hash_t);
};
};
#endif

