// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_DIRECTORY_H_
#define INCLUDE_DIRECTORY_H_

#include <list>
#include <unordered_map>
#include "settings.h"
#include "hashtable.h"
#include "map.h"

enum DirState {
  DIR_UNSHARED,
  DIR_DIRTY,
  DIR_SHARED,
  DIR_TO_DIRTY,
  DIR_TO_SHARED,
  DIR_TO_UNSHARED
};

struct DirEntry {
  DirState state = DIR_UNSHARED;
  list<GAddr> shared;
  ptr_t addr;
  //if lock == 0, no one is holding the lock. otherwise, there are #lock ones holding the lock
  //but if lock = EXCLUSIVE_LOCK_TAG, it is a exclusive lock
  //int lock = 0;
  unordered_map<ptr_t, int> locks;
};

class Directory {
  /*
   * the directory structure
   * key: BLOCK-aligned address
   * value: directory entry
   */
  //unordered_map<ptr_t, DirEntry> dir;
#ifdef USE_SIMPLE_MAP
  Map<ptr_t, DirEntry*> dir { "directory" };
#else
  HashTable<ptr_t, DirEntry*> dir {"directory"};
#endif

  DirState GetState(ptr_t ptr);bool InTransitionState(ptr_t ptr);
  int RLock(ptr_t ptr);
  int RLock(DirEntry* entry, ptr_t ptr);bool IsRLocked(ptr_t ptr);bool IsRLocked(
      DirEntry* entry, ptr_t ptr);
  int WLock(ptr_t ptr);
  int WLock(DirEntry* entry, ptr_t ptr);bool IsWLocked(ptr_t ptr);bool IsWLocked(
      DirEntry* entry, ptr_t ptr);

  DirEntry* GetEntry(ptr_t ptr) {
    if (dir.count(TOBLOCK(ptr))) {
      return dir.at(TOBLOCK(ptr));
    } else {
      return nullptr;
    }
  }

  void UnLock(ptr_t ptr);
  void UnLock(DirEntry*& entry, ptr_t ptr);bool IsBlockWLocked(ptr_t block);bool IsBlockLocked(
      ptr_t block);
  void Clear(ptr_t ptr, GAddr addr);
  inline list<GAddr>& GetSList(ptr_t ptr) {
    return dir.at(TOBLOCK(ptr))->shared;
  }
  inline void lock(ptr_t ptr) {
    epicAssert(BLOCK_ALIGNED(ptr));
    LOCK_MICRO(dir, ptr);
  }
  inline void unlock(ptr_t ptr) {
    epicAssert(BLOCK_ALIGNED(ptr));
    UNLOCK_MICRO(dir, ptr);
  }
 public:
  /*
   * @ptr is the local virtual address
   */
  inline DirState GetState(void* ptr) {
    return GetState((ptr_t) ptr);
  }
  inline DirState GetState(DirEntry* entry) {
    if (!entry) {
      return DIR_UNSHARED;
    } else {
      return entry->state;
    }
  }
  inline bool InTransitionState(void* ptr) {
    return InTransitionState((ptr_t) ptr);
  }
  inline bool InTransitionState(DirState s) {
    return s == DIR_TO_DIRTY || s == DIR_TO_SHARED || s == DIR_TO_UNSHARED;
  }
  inline bool InTransitionState(DirEntry* entry) {
    if (!entry) {
      return false;
    } else {
      return InTransitionState(entry->state);
    }
  }
  DirEntry* GetEntry(void* ptr) {
    return GetEntry((ptr_t) ptr);
  }

  DirEntry* ToShared(void* ptr, GAddr addr);
  void ToShared(DirEntry* entry, GAddr addr);
  DirEntry* ToDirty(void* ptr, GAddr addr);
  void ToDirty(DirEntry* entry, GAddr addr);
  void ToUnShared(void* ptr);
  void ToUnShared(DirEntry*& entry);
  void ToToShared(void* ptr, GAddr addr = Gnullptr);
  void ToToShared(DirEntry* entry, GAddr addr = Gnullptr);
  DirEntry* ToToDirty(void* ptr, GAddr addr = Gnullptr);
  inline void ToToDirty(DirEntry* entry, GAddr = Gnullptr) {
    epicAssert(entry);
    entry->state = DIR_TO_DIRTY;
  }
  void ToToUnShared(void* ptr);
  void ToToUnShared(DirEntry* entry);
  void UndoDirty(void* ptr);
  void UndoDirty(DirEntry* entry);
  void UndoShared(void* ptr);
  void UndoShared(DirEntry* entry);
  void Remove(void* ptr, int wid);
  void Remove(DirEntry*& entry, int wid);

  inline int RLock(void* ptr) {
    return RLock((ptr_t) ptr);
  }
  inline int RLock(DirEntry* entry, void* ptr) {
    return RLock(entry, (ptr_t) ptr);
  }
  int RLock(DirEntry* entry) = delete;

  inline bool IsRLocked(void* ptr) {
    return IsRLocked((ptr_t) ptr);
  }
  inline bool IsRLocked(DirEntry* entry, void* ptr) {
    return IsRLocked(entry, (ptr_t) ptr);
  }
  bool IsRLocked(DirEntry* entry) = delete;

  inline int WLock(void* ptr) {
    return WLock((ptr_t) ptr);
  }
  inline int WLock(DirEntry* entry, void* ptr) {
    return WLock(entry, (ptr_t) ptr);
  }
  int WLock(DirEntry* entry) = delete;

  inline bool IsWLocked(void* ptr) {
    return IsWLocked((ptr_t) ptr);
  }
  inline bool IsWLocked(DirEntry* entry, void* ptr) {
    return IsWLocked(entry, (ptr_t) ptr);
  }
  bool IsWLocked(DirEntry* entry) = delete;

  inline void UnLock(void* ptr) {
    UnLock((ptr_t) ptr);
  }
  inline void UnLock(DirEntry*& entry, void* ptr) {
    UnLock(entry, (ptr_t) ptr);
  }
  void UnLock(DirEntry* entry) = delete;

  inline bool IsBlockWLocked(void* block) {
    return IsBlockWLocked((ptr_t) block);
  }
  bool IsBlockWLocked(DirEntry* entry);
  inline bool IsBlockLocked(void* block) {
    return IsBlockLocked((ptr_t) block);
  }
  bool IsBlockLocked(DirEntry* entry);

  inline void Clear(void* ptr, GAddr addr) {
    Clear((ptr_t) ptr, addr);
  }
  void Clear(DirEntry*& entry, GAddr addr);

  inline list<GAddr>& GetSList(void* ptr) {
    return GetSList((ptr_t) ptr);
  }
  list<GAddr>& GetSList(DirEntry* entry) {
    return entry->shared;
  }

  //below are used for multithread programming
  inline void lock(void* ptr) {
    lock((ptr_t) ptr);
  }
  inline void unlock(void* ptr) {
    unlock((ptr_t) ptr);
  }
  void unlock(DirEntry* entry) = delete;
  void lock(DirEntry* entry) = delete;
};

#endif /* INCLUDE_DIRECTORY_H_ */
