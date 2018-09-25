// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_LOCKWRAPPER_H_
#define INCLUDE_LOCKWRAPPER_H_

#include <mutex>
#include "settings.h"

class LockWrapper {
#ifdef USE_ATOMIC
  bool lock_ = 0;
#else
  std::mutex lock_;
#endif

 public:
  inline void lock() {
#ifdef USE_ATOMIC
    while (__atomic_test_and_set(&lock_, __ATOMIC_RELAXED))
      ;
#else
    lock_.lock();
#endif
  }

  inline void unlock() {
#ifdef USE_ATOMIC
    __atomic_clear(&lock_, __ATOMIC_RELAXED);
#else
    lock_.unlock();
#endif
  }

  inline bool try_lock() {
#ifdef USE_ATOMIC
    return !__atomic_test_and_set(&lock_, __ATOMIC_RELAXED);
#else
    return lock_.try_lock();
#endif
  }

};

#endif /* INCLUDE_LOCKWRAPPER_H_ */
