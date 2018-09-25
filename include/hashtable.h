// Copyright (c) 2018 The GAM Authors 

#ifndef INCLUDE_HASHTABLE_H_
#define INCLUDE_HASHTABLE_H_

#include <mutex>
#include <array>
#include <string>
#include <exception>
#include <assert.h>

#include "lockwrapper.h"
#include "settings.h"
#include "log.h"

#ifdef USE_COCKOOHASH

#include "../lib/libcuckoo/src/cuckoohash_map.hh"
#include "../lib/libcuckoo/src/city_hasher.hh"
#include "murmur_hasher.h"

using std::mutex;
using std::array;
using std::string;
using std::exception;

const size_t lock_array_size = 1 << 16;

template<typename Key, typename T>
#ifdef USE_CITYHASH
class HashTable : public cuckoohash_map<Key, T, CityHasher<Key>> {
#elif defined(USE_MURMURHASH)
  class HashTable: public cuckoohash_map<Key, T, MurmurHasher<Key>> {
#else
    class HashTable: public cuckoohash_map<Key, T> {
#endif

 public:
  HashTable(string name = "DEFAULT_HASHTABLE_NAME", size_t n = DEFAULT_SIZE,
            double mlf = DEFAULT_MINIMUM_LOAD_FACTOR, size_t mhp =
                NO_MAXIMUM_HASHPOWER)
      : name(name),
#ifdef USE_CITYHASH
        cuckoohash_map<Key, T, CityHasher<Key>>::cuckoohash_map(n, mlf, mhp)
#elif defined(USE_MURMURHASH)
  cuckoohash_map<Key, T, MurmurHasher<Key>>::cuckoohash_map(n, mlf, mhp)
#else
  cuckoohash_map<Key, T>::cuckoohash_map(n, mlf, mhp)
#endif
  {
  }
  void lock(Key key);
  void unlock(Key key);bool try_lock(Key key);
  inline size_t count(Key key) {
    return this->contains(key);
  }
  //overwrite the default at that returns a reference
  inline T at(const Key& key) {
    T ret;
    try {
      ret = this->find(key);
    } catch (const exception& e) {
      printf("cannot find the key for hash table %s (%s)", name.c_str(),
             e.what());
      assert(false);
    }
    return ret;
  }

 private:
  array<LockWrapper, lock_array_size> lock_;
  string name;
};

template<typename Key, typename T>
inline void HashTable<Key, T>::lock(Key key) {
  //epicLog(LOG_DEBUG, "trying to lock %s", name.c_str());
#ifdef USE_CITYHASH
  lock_[cuckoohash_map<Key, T, CityHasher<Key>>::hash_function()(key)
      % lock_array_size].lock();
#elif defined(USE_MURMURHASH)
  lock_[cuckoohash_map<Key, T, MurmurHasher<Key>>::hash_function()(key) % lock_array_size].lock();
#else
  lock_[cuckoohash_map<Key, T>::hash_function()(key) % lock_array_size].lock();
#endif
  //epicLog(LOG_DEBUG, "locked %s", name.c_str());
}

template<typename Key, typename T>
inline bool HashTable<Key, T>::try_lock(Key key) {
  //epicLog(LOG_DEBUG, "trying to lock %s", name.c_str());
#ifdef USE_CITYHASH
  return lock_[cuckoohash_map<Key, T, CityHasher<Key>>::hash_function()(key)
      % lock_array_size].try_lock();
#elif defined(USE_MURMURHASH)
  return lock_[cuckoohash_map<Key, T, MurmurHasher<Key>>::hash_function()(key) % lock_array_size].try_lock();
#else
  return lock_[cuckoohash_map<Key, T>::hash_function()(key) % lock_array_size].try_lock();
#endif
  //epicLog(LOG_DEBUG, "locked %s", name.c_str());
}

template<typename Key, typename T>
inline void HashTable<Key, T>::unlock(Key key) {
#ifdef USE_CITYHASH
  lock_[cuckoohash_map<Key, T, CityHasher<Key>>::hash_function()(key)
      % lock_array_size].unlock();
#elif defined(USE_MURMURHASH)
  lock_[cuckoohash_map<Key, T, MurmurHasher<Key>>::hash_function()(key) % lock_array_size].unlock();
#else
  lock_[cuckoohash_map<Key, T>::hash_function()(key) % lock_array_size].unlock();
#endif
  //epicLog(LOG_DEBUG, "trying to unlock %s", name.c_str());
}

#else

template<typename Key, typename T>
class HashTable: public std::unordered_map<Key, T> {
public:
  HashTable(string name = "DEFAULT_HASHTABLE_NAME") {
    this->name = name;
  }
  void lock(Key key);
  void unlock(Key key);

private:
  mutex lock_;
  string name;
};

template<typename Key, typename T>
inline void HashTable<Key, T>::lock(Key key) {
//		epicLog(LOG_DEBUG, "trying to lock %s", name.c_str());
  lock_.lock();
//		epicLog(LOG_DEBUG, "locked %s", name.c_str());
}

template<typename Key, typename T>
inline void HashTable<Key, T>::unlock(Key key) {
//		epicLog(LOG_DEBUG, "trying to lock %s", name.c_str());
  lock_.unlock();
//		epicLog(LOG_DEBUG, "locked %s", name.c_str());
}

#endif

#endif

