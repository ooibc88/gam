// Copyright (c) 2018 The GAM Authors 
 
#ifndef KV_H_
#define KV_H_

#include "../include/structure.h"
#include "chars.h"
#include "../include/log.h"

namespace dht{

using hash_t = uint64_t;
using hkey_t = char*;

#define HASH_TABLE_NOT_EXIST 1

#define BKT_SIZE 128
#define ENTRY_SIZE 12 // 12bit tag + 20bit size + 64bit value
#define BKT_SLOT (BKT_SIZE / ENTRY_SIZE)

#define TAG_BITS 11 
#define TAG_MASK ((1UL << TAG_BITS) - 1)
#define TAG(hash) (hash & TAG_MASK) 

#define BKT_BITS 24
#define NBKT (1UL << BKT_BITS)
#define BKT_MASK ((1UL << BKT_BITS) - 1)
#define BUCKET(hash) ((hash >> TAG_BITS) & BKT_MASK)

#define TBL_BITS (64 - TAG_BITS - BKT_BITS)
#define TBL(hash) ((hash >> (64 - TBL_BITS)))

#define SZ_BITS 20
#define SZ_MASK ((1 << SZ_BITS) - 1)

#define MAX_KEY_SIZE 1024
#define MAX_VAL_SIZE 16384
#define MAX_KV_SIZE (8 + MAX_KEY_SIZE + MAX_VAL_SIZE)

static void parseEntry(char* entry, uint32_t& tag, uint32_t& size, GAddr& addr) {
  readInteger(entry, tag, addr);
  tag &= ((1UL << 31) - 1);
  size = (tag & SZ_MASK);
  tag = (tag >> SZ_BITS);
}

static void updateEntry(char* entry, uint32_t tag, uint32_t size, GAddr addr) {
  uint32_t etag = (1UL << 31) | (tag << SZ_BITS) | (size & SZ_MASK);
  appendInteger(entry, etag, addr);
}

//static int getTag(char* entry, uint32_t& tag) {
//    readInteger(entry, tag);
//    tag &= ((1UL << 31 ) - 1);
//    return (tag & (1UL<<31));
//}

static bool matchTag(char* entry, uint32_t tag) {
  uint32_t etag;
  readInteger(entry, etag);
  if (etag == 0) return false;
  etag &= ((1UL<<31)-1);
  etag = (etag >> SZ_BITS);
  return tag == etag; 
}

static bool containTag(char* entry) {
  uint32_t tag;
  readInteger(entry, tag);
  return tag != 0;
}

};

#endif
