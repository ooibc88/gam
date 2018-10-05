#ifndef __DATABASE_META_H__
#define __DATABASE_META_H__

#include <cstring>
#include <cstdint>

#include "gallocator.h"

namespace Database {
typedef uint32_t HashcodeType;
typedef uint64_t IndexKey;

enum LockType
  : size_t {NO_LOCK,
  READ_LOCK,
  WRITE_LOCK,
};
enum AccessType
  : size_t {READ_ONLY,
  READ_WRITE,
  INSERT_ONLY,
  DELETE_ONLY
};
enum SourceType
  : size_t {RANDOM_SOURCE,
  PARTITION_SOURCE
};

// storage
const size_t kMaxTableNum = 16;
const size_t kMaxColumnNum = 32;
const size_t kMaxSecondaryIndexNum = 5;
const uint64_t kHashIndexBucketHeaderNum = 1000007;
// txn
const size_t kTryLockLimit = 1;
const size_t kMaxAccessLimit = 256;

extern GAlloc* default_gallocator;
extern GAlloc** gallocators;
extern size_t gThreadCount;

// source
extern size_t gParamBatchSize;

}

#endif
