// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_MAP_H_
#define INCLUDE_MAP_H_
#include <array>
#include <vector>
#include <list>
#include <mutex>
#include "default_hasher.hh"
#include "settings.h"
#include "city_hasher.hh"
#include "lockwrapper.h"
#include "murmur_hasher.h"

#define DEFAULT_BUCKET_SIZE_POWER (21)
//#define BUCKET_SIZE_CHECK
#ifdef BUCKET_SIZE_CHECK
#define WARNING_SIZE 5
#endif

template<typename key_type, typename mapped_type, size_t BUCKET_SIZE_POWER =
DEFAULT_BUCKET_SIZE_POWER,
#ifdef USE_CITYHASH
    typename Hash = CityHasher<key_type>,
#elif defined(USE_MURMURHASH)
    typename Hash = MurmurHasher<key_type>,
#else
    typename Hash = DefaultHasher<key_type>,
#endif
    class Pred = std::equal_to<key_type>, class Alloc = std::allocator<
        std::pair<const key_type, mapped_type>>>class Map {
  typedef std::pair<key_type, mapped_type> storage_value_type;
  class Bucket {
    LockWrapper lock_;
  public:
    std::list<storage_value_type> kvs;
    inline void lock() {
      lock_.lock();
    }
    inline bool try_lock() {
      return lock_.try_lock();
    }
    inline void unlock() {
      lock_.unlock();
    }
  };

  Bucket** buckets_;
  uint64_t hashmask;
  std::string name;

  // hashfn returns an instance of the hash function
  static Hash hashfn() {
    static Hash hash;
    return hash;
  }

  // eqfn returns an instance of the equality predicate
  static Pred eqfn() {
    static Pred eq;
    return eq;
  }

public:
  Map(std::string&& name = "DEFAULT_HASHTABLE_NAME") {
    hashmask = (1 << BUCKET_SIZE_POWER) - 1;
    long bucket_size = 1 << BUCKET_SIZE_POWER;
    buckets_ = (Bucket**)malloc(bucket_size * sizeof(Bucket*));
    for (int i = 0; i < bucket_size; i++) {
      buckets_[i] = new Bucket();
    }
    this->name = std::move(name);
  }

  size_t lock(const key_type& k) {
    size_t hv = hashfn()(k) & hashmask;
    buckets_[hv]->lock();
    return hv;
  }

  bool try_lock(const key_type& k) {
    size_t hv = hashfn()(k) & hashmask;
    return buckets_[hv]->try_lock();
  }

  void unlock(const key_type& k) {
    size_t hv = hashfn()(k) & hashmask;
    buckets_[hv]->unlock();
  }

  void unlock_hv(size_t hv) {
    buckets_[hv]->unlock();
  }

  mapped_type& operator[](const key_type& k) {
    size_t hv = hashfn()(k) & hashmask;
    Bucket* b = buckets_[hv];
#ifdef BUCKET_SIZE_CHECK
    if(b->kvs.size() > WARNING_SIZE) {
      printf("bucket size (%s) = %ld\n", name.c_str(), b->kvs.size());
    }
#endif
    for (storage_value_type& kv : b->kvs) {
      if (eqfn()(kv.first, k)) {
        return kv.second;
      }
    }
    b->kvs.push_back(storage_value_type(k, mapped_type {}));
    return b->kvs.back().second;
  }

  mapped_type& at(const key_type& k) {
    size_t hv = hashfn()(k) & hashmask;
    Bucket* b = buckets_[hv];
#ifdef BUCKET_SIZE_CHECK
    if(b->kvs.size() > WARNING_SIZE) {
      printf("bucket size (%s) = %ld\n", name.c_str(), b->kvs.size());
    }
#endif
    for (storage_value_type& kv : b->kvs) {
      if (eqfn()(kv.first, k)) {
        return kv.second;
      }
    }
    throw std::out_of_range("key not found in table");
    //return mapped_type{};
  }

  size_t count(const key_type& k) {
    size_t hv = hashfn()(k) & hashmask;
    Bucket* b = buckets_[hv];
#ifdef BUCKET_SIZE_CHECK
    if(b->kvs.size() > WARNING_SIZE) {
      printf("bucket size (%s) = %ld\n", name.c_str(), b->kvs.size());
    }
#endif
    for (storage_value_type& kv : b->kvs) {
      if (eqfn()(kv.first, k)) {
        return 1;
      }
    }
    return 0;
  }

  mapped_type& operator[](key_type&& k) {
    size_t hv = hashfn()(k) & hashmask;
    Bucket* b = buckets_[hv];
#ifdef BUCKET_SIZE_CHECK
    if(b->kvs.size() > WARNING_SIZE) {
      printf("bucket size (%s) = %ld\n", name.c_str(), b->kvs.size());
    }
#endif
    for (storage_value_type& kv : b->kvs) {
      if (eqfn()(kv.first, k)) {
        return kv.second;
      }
    }
    b->kvs.push_back(storage_value_type(std::move(k), mapped_type {}));
    return b->kvs.back().second;
  }

  size_t erase(const key_type& k) {
    size_t hv = hashfn()(k) & hashmask;
    Bucket* b = buckets_[hv];
#ifdef BUCKET_SIZE_CHECK
    if(b->kvs.size() > WARNING_SIZE) {
      printf("bucket size (%s) = %ld\n", name.c_str(), b->kvs.size());
    }
#endif
    for (auto it = b->kvs.begin(); it != b->kvs.end(); ++it) {
      if (eqfn()(it->first, k)) {
        b->kvs.erase(it);
        return 1;
      }
    }
    return 0;
  }
};

#endif /* INCLUDE_MAP_H_ */
