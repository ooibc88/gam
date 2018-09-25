// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_MURMUR_HASHER_H_
#define INCLUDE_MURMUR_HASHER_H_

#include <string>
#include "MurmurHash.h"

static uint32_t seed = 123456;

/*! CityHasher is a std::hash-style wrapper around CityHash. We
 *  encourage using CityHasher instead of the default std::hash if
 *  possible. */
template<class Key>
class MurmurHasher {
 public:
  size_t operator()(const Key& k) const {
    return MurmurHash2((const char*) &k, sizeof(k), seed);
  }
};

/*! This is a template specialization of CityHasher for
 *  std::string. */
template<>
class MurmurHasher<std::string> {
 public:
  size_t operator()(const std::string& k) const {
    return MurmurHash2(k.c_str(), k.size(), seed);
  }
};

#endif /* INCLUDE_MURMUR_HASHER_H_ */
