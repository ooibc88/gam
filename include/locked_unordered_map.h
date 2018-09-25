// Copyright (c) 2018 The GAM Authors 

#ifndef INCLUDE_LOCKEDUNORDEREDMAP_H_
#define INCLUDE_LOCKEDUNORDEREDMAP_H_

#include <mutex>
#include <array>
#include <string>
#include <exception>
#include <unordered_map>
#include "../lib/libcuckoo/src/cuckoohash_map.hh"
#include "lockwrapper.h"

using std::mutex;
using std::array;
using std::string;
using std::exception;

template<typename Key, typename T>
class UnorderedMap : public std::unordered_map<Key, T> {
 public:
  UnorderedMap(string name = "DEFAULT_HASHTABLE_NAME") {
    this->name = name;
  }
  void lock(Key key);
  void unlock(Key key);

 private:
  LockWrapper lock_;
  string name;
};

template<typename Key, typename T>
inline void UnorderedMap<Key, T>::lock(Key key) {
//		epicLog(LOG_DEBUG, "trying to lock %s", name.c_str());
//		lock_.lock();
//		epicLog(LOG_DEBUG, "locked %s", name.c_str());
}

template<typename Key, typename T>
inline void UnorderedMap<Key, T>::unlock(Key key) {
//		epicLog(LOG_DEBUG, "trying to lock %s", name.c_str());
//		lock_.unlock();
//		epicLog(LOG_DEBUG, "locked %s", name.c_str());
}

#endif

