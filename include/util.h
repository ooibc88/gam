// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_UTIL_H_
#define INCLUDE_UTIL_H_

#include <vector>
#include <sstream>
#include <syscall.h>
#include "settings.h"
#include "structure.h"

template<class T>
inline vector<T>& Split(stringstream& ss, vector<T>& elems, char delim) {
  T item;
  while (ss >> item) {
    elems.push_back(item);
    if (ss.peek() == delim)
      ss.ignore();
  }
  return elems;
}

template<class T>
inline vector<T>& Split(string& s, vector<T>& elems, char delim =
                            DEFAULT_SPLIT_CHAR) {
  stringstream ss(s);
  return Split(ss, elems, delim);
}

template<class T>
inline vector<T>& Split(char *s, vector<T>& elems, char delim =
                            DEFAULT_SPLIT_CHAR) {
  stringstream ss(s);
  return Split(ss, elems, delim);
}

template<>
vector<string>& Split<string>(stringstream& ss, vector<string>& elems,
                              char delim);

string get_local_ip(const char* iface = nullptr);
inline string get_local_ip(const string iface = "") {
  return get_local_ip(iface.empty() ? nullptr : iface.c_str());
}

long get_time();
uint64_t rdtsc();

#define atomic_add(v, i) __sync_fetch_and_add((v), (i))
#define atomic_read(v) __sync_fetch_and_add((v), (0))

inline int GetRandom(int min, int max) {
  static thread_local unsigned int tid = (unsigned int) syscall(SYS_gettid);
  epicLog(LOG_DEBUG, "tid = %d", tid);
  int ret = (rand_r(&tid) % (max - min)) + min;
  return ret;
}

inline uint64_t ceil_divide(uint64_t a, uint64_t b) {
  return (a + b - 1) / b;
}

inline int GetRandom(int min, int max, unsigned int* seedp) {
  int ret = (rand_r(seedp) % (max - min)) + min;
  return ret;
}

/*
 * only support basic types
 */
template<class T1, class T2>
inline T1 force_cast(T2 t2) {
  assert(sizeof(T1) == sizeof(T2));
  T1 t1;
  memcpy(&t1, &t2, sizeof(T1));
  return t1;
}

#endif /* INCLUDE_UTIL_H_ */
