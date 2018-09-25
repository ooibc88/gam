// Copyright (c) 2018 The GAM Authors 


#include "structure.h"
#include "directory.h"

DirState Directory::GetState(ptr_t ptr) {
  DirState s = DIR_UNSHARED;
  if (dir.count(TOBLOCK(ptr))) {
    s = dir.at(TOBLOCK(ptr))->state;
  }
  return s;
}

bool Directory::InTransitionState(ptr_t ptr) {
  bool ret = false;
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  if (dir.count(block)) {
    DirEntry* entry = dir.at(block);
    epicLog(LOG_DEBUG, "found the directory entry for %lx, state = %d", block,
            entry->state);
    ret = entry->state == DIR_TO_DIRTY || entry->state == DIR_TO_SHARED
        || entry->state == DIR_TO_UNSHARED;
  }
  return ret;
}

void Directory::ToShared(DirEntry* entry, GAddr addr) {
  epicAssert(entry);
  if (entry->state == DIR_TO_SHARED) {
    epicAssert(entry->shared.size() == 1);
    /*
     * if addr == Gnullptr, then a writeback request
     * else, addr == shared.front();
     */
    if (addr != Gnullptr) {
      epicAssert(addr == entry->shared.front());
      entry->shared.pop_front();
      entry->shared.push_back(addr);
    }
    entry->state = DIR_SHARED;
  } else {
    epicAssert((entry->state == DIR_SHARED && entry->shared.size() > 0)
            || (entry->state == DIR_UNSHARED && IsBlockLocked(entry)
            && !IsBlockWLocked(entry)));
    entry->state = DIR_SHARED;
    if (!(entry->shared.size() == 0
        || (entry->shared.front() != addr
        && WID(entry->shared.front()) != WID(addr)))) {
      epicLog(LOG_WARNING, "size = %d, front = %lx, current = %lx",
              entry->shared.size(), entry->shared.front(), addr);
      epicAssert(false);
    }
    entry->shared.push_back(addr);
  }
}

/*
 * if curr_state = DIRTY and wid = the owner worker, remote can be null
 * otherwise, remote cannot be null
 */
DirEntry* Directory::ToShared(void* ptr, GAddr addr) {
  //TODO: add a check on the max shared entries to 128 (i.e., MAX_UNSIGNED_CHAR)
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  if (!entry) {
    entry = new DirEntry();
    entry->state = DIR_SHARED;
    entry->shared.push_back(addr);
    entry->addr = block;
    dir[block] = entry;
  } else {
    ToShared(entry, addr);
  }
  return entry;
}

void Directory::Remove(DirEntry*& entry, int wid) {
  if (!entry) {
    epicLog(LOG_WARNING, "remove an empty entry");
    return;
  }
  epicAssert(entry->shared.size() >= 1);
  bool found = false;
  for (auto it = entry->shared.begin(); it != entry->shared.end(); it++) {
    if (WID(*it) == wid) {
      entry->shared.erase(it);
      found = true;
      break;
    }
  }
  epicAssert(found);
  if (entry->shared.size() == 0 && !IsBlockLocked(entry)) {
    int ret = dir.erase(entry->addr);
    epicAssert(ret);
    delete entry;
    entry = nullptr;
  }
}

void Directory::Remove(void* ptr, int wid) {
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  epicAssert(entry);
  Remove(entry, wid);
}

void Directory::UndoShared(DirEntry* entry) {
  epicAssert(entry);
  epicAssert(InTransitionState(entry->state));
  entry->state = DIR_SHARED;
}

void Directory::UndoShared(void* ptr) {
  epicLog(LOG_DEBUG, "UndoShared");
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  try {
    DirEntry* entry = dir.at(block);
    UndoShared(entry);
  } catch (const exception& e) {
    epicLog(LOG_FATAL, "Unexpected: cannot find the directory entry");
    epicAssert(false);
  }
}

void Directory::ToUnShared(DirEntry*& entry) {
  if (!entry)
    return;
  epicLog(LOG_DEBUG, "dir.at(TOBLOCK(ptr)).state = %d", entry->state);
#ifndef SELECTIVE_CACHING
  epicAssert(entry->state == DIR_TO_UNSHARED);
#endif
  if (IsBlockLocked(entry)) {
    entry->state = DIR_UNSHARED;
    entry->shared.clear();
    epicLog(LOG_DEBUG, "dir is locked, just change it to dir_unshared");
  } else {
    if (!dir.erase(entry->addr)) {
      epicLog(LOG_WARNING, "cannot unshared the directory entry");
    }
    delete entry;
    entry = nullptr;
  }
}

void Directory::ToUnShared(void* ptr) {
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  DirEntry* entry = GetEntry(ptr);
  if (!entry)
    return;
  ToUnShared(entry);
}

void Directory::ToDirty(DirEntry* entry, GAddr addr) {
  epicAssert(entry);
  epicAssert(
      entry->state == DIR_UNSHARED || entry->state == DIR_SHARED
          || entry->state == DIR_TO_DIRTY);
  entry->state = DIR_DIRTY;
  entry->shared.clear();
  entry->shared.push_back(addr);
}

DirEntry* Directory::ToDirty(void* ptr, GAddr addr) {
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(ptr);
  if (!entry) {
    entry = new DirEntry();
    entry->state = DIR_DIRTY;
    entry->shared.push_back(addr);
    entry->addr = block;
    dir[block] = entry;
  } else {
    ToDirty(entry, addr);
  }
  return entry;
}

void Directory::UndoDirty(DirEntry* entry) {
  epicAssert(entry);
  epicLog(LOG_DEBUG, "UndoDirty");
  epicAssert(InTransitionState(entry->state));
  entry->state = DIR_DIRTY;
}

void Directory::UndoDirty(void* ptr) {
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  try {
    DirEntry* entry = dir.at(block);
    UndoDirty(entry);
  } catch (const exception& e) {
    epicLog(LOG_FATAL, "Unexpected: cannot find the directory entry");
    epicAssert(false);
  }
}

void Directory::ToToShared(DirEntry* entry, GAddr addr) {
  epicAssert(entry);
  epicAssert(entry->state == DIR_DIRTY);  //only transmission from dirty to shared needs this intermediate state
  epicAssert(entry->shared.size() == 1);
  entry->state = DIR_TO_SHARED;
}

/*
 * intermediate state
 * only happen in transition from dirty to shared
 * READ Case 1, Case 3
 */
void Directory::ToToShared(void* ptr, GAddr addr) {
  //TODO: add a check on the max shared entries to 128 (i.e., MAX_UNSIGNED_CHAR)
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  ToToShared(entry, addr);
}

void Directory::ToToUnShared(DirEntry* entry) {
  epicAssert(entry);
  entry->state = DIR_TO_UNSHARED;
  epicLog(LOG_DEBUG, "change to dir_to_unshared");
}

/*
 * intermediate state
 * in transition from dirty/shared to unshared
 * WRITE Case 1 (shared to unshared), WRITE Case 2 (dirty to unshared)
 */
void Directory::ToToUnShared(void* ptr) {
  ptr_t block = TOBLOCK(ptr);
  epicAssert((ptr_t )ptr == block);
  DirEntry* entry = GetEntry(block);
  ToToUnShared(entry);
}

/*
 * intermediate state
 * in transition from dirty(ownership transfer) to dirty
 * WRITE Case 4 (dirty to dirty)
 * NOTE: shared/unshared to dirty (WRITE Case 3) doesn't need this state
 * as we cannot update to_dirty to dirty in this cases.
 * we use cache check to avoid races
 */
DirEntry* Directory::ToToDirty(void* ptr, GAddr addr) {
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  if (!entry) {  //unshared to dirty
    entry = new DirEntry();
    entry->state = DIR_TO_DIRTY;
    entry->addr = block;
    dir[block] = entry;
  } else {
    ToToDirty(entry);
  }
  return entry;
}

int Directory::RLock(DirEntry* entry, ptr_t ptr) {
  epicAssert(entry);
  epicAssert(!InTransitionState(entry));
  if (IsWLocked(entry, ptr)) {
    return -1;
  }
  if (entry->locks.count(ptr)) {
    entry->locks[ptr]++;
  } else {
    entry->locks[ptr] = 1;
  }
  //TODO: add max check and handle
  epicAssert(entry->locks[ptr] <= MAX_SHARED_LOCK);
  epicAssert(IsBlockLocked(entry));
  return 0;
}

int Directory::RLock(ptr_t ptr) {
  //epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  if (!entry) {
    entry = new DirEntry();
    entry->state = DIR_UNSHARED;  //actually this state only happens when locked
    entry->locks[ptr] = 1;
    entry->addr = block;
    dir[block] = entry;
    epicAssert(IsBlockLocked(entry));
    return 0;
  } else {
    return RLock(entry, ptr);
  }
}

int Directory::WLock(DirEntry* entry, ptr_t ptr) {
  epicAssert(entry);
  epicAssert(!InTransitionState(entry));
  if (IsWLocked(entry, ptr) || IsRLocked(entry, ptr)) {
    return -1;
  }
  epicAssert(entry->state == DIR_UNSHARED);
  entry->locks[ptr] = EXCLUSIVE_LOCK_TAG;
  epicAssert(IsBlockWLocked(entry) && IsBlockLocked(entry));
  return 0;
}

/*
 * we should call ToUnshared before call WLock
 */
int Directory::WLock(ptr_t ptr) {
  //epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  if (!entry) {
    entry = new DirEntry();
    entry->state = DIR_UNSHARED;  //actually this state only happens when locked
    entry->locks[ptr] = EXCLUSIVE_LOCK_TAG;
    entry->addr = block;
    dir[block] = entry;
    epicAssert(IsBlockWLocked(entry) && IsBlockLocked(entry));
    return 0;
  } else {
    return WLock(entry, ptr);
  }
}

bool Directory::IsWLocked(DirEntry* entry, ptr_t ptr) {
  if (!entry)
    return false;
  return entry->state == DIR_UNSHARED && entry->locks.count(ptr)
      && entry->locks.at(ptr) == EXCLUSIVE_LOCK_TAG;
}

bool Directory::IsWLocked(ptr_t ptr) {
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  if (!entry) {
    return false;
  } else {
    return IsWLocked(entry, ptr);
  }
}

bool Directory::IsRLocked(DirEntry* entry, ptr_t ptr) {
  if (!entry)
    return false;
  return entry->locks.count(ptr) && entry->locks.at(ptr) > 0
      && entry->locks.at(ptr) != EXCLUSIVE_LOCK_TAG;
}

bool Directory::IsRLocked(ptr_t ptr) {
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  if (!dir.count(block)) {
    return false;
  } else {
    return IsRLocked(entry, ptr);
  }
}

bool Directory::IsBlockWLocked(DirEntry* entry) {
  if (!entry)
    return false;
  bool wlocked = false;
  for (auto& en : entry->locks) {
    if (en.second == EXCLUSIVE_LOCK_TAG) {
      wlocked = true;
      break;
    }
  }
  return wlocked;
}

bool Directory::IsBlockWLocked(ptr_t block) {
  epicAssert(block == TOBLOCK(block));
  DirEntry* entry = GetEntry(block);
  if (!entry) {
    return false;
  } else {
    return IsBlockWLocked(entry);
  }
}

bool Directory::IsBlockLocked(DirEntry* entry) {
  if (!entry)
    return false;
  return entry->locks.size() > 0 ? true : false;
}

bool Directory::IsBlockLocked(ptr_t block) {
  epicAssert(block == TOBLOCK(block));
  DirEntry* entry = GetEntry(block);
  if (!entry) {
    return false;
  } else {
    return IsBlockLocked(entry);
  }
}

void Directory::UnLock(DirEntry*& entry, ptr_t ptr) {
  epicLog(LOG_DEBUG, "entry->state = %d, entry->locks.size() = %d",
          entry->state, entry->locks.size());
  epicAssert(entry->state == DIR_UNSHARED
             || ((entry->state == DIR_SHARED || entry->state == DIR_TO_UNSHARED)
             && IsRLocked(ptr)));
  epicAssert(entry->locks.count(ptr) && entry->locks.at(ptr) > 0);
  entry->locks[ptr] = IsWLocked(entry, ptr) ? 0 : entry->locks[ptr] - 1;
  if (entry->locks[ptr] == 0) {
    entry->locks.erase(ptr);
  }
  if (entry->state == DIR_UNSHARED && entry->locks.size() == 0) {
    dir.erase(entry->addr);
    delete entry;
    entry = nullptr;
  }
}

void Directory::UnLock(ptr_t ptr) {
  ptr_t block = TOBLOCK(ptr);
  try {
    DirEntry* entry = dir.at(block);
    UnLock(entry, ptr);
  } catch (const exception& e) {
    epicLog(LOG_FATAL, "Unexpected: cannot find the directory entry");
    epicAssert(false);
  }
}

void Directory::Clear(DirEntry*& entry, GAddr addr) {
  if (!entry) {
    epicLog(LOG_WARNING,
        "there must be a race before, and it has already been invalidated (%lx)", addr);
    return;
  }
  if (entry->state == DIR_DIRTY) {
    epicAssert(entry->shared.size() == 1);
    if (addr == entry->shared.front()) {
      entry->shared.pop_front();
    } else {
      epicLog(LOG_WARNING,
              "there must be a race before, and new owner change from %d to %d",
              WID(addr), WID(entry->shared.front()));
    }
  } else {
    epicAssert((entry->state == DIR_SHARED && entry->shared.size() > 0)
            || (entry->state == DIR_UNSHARED && IsBlockLocked(entry)));
    entry->shared.remove(addr);
  }
  if (entry->shared.size() == 0) {
    if (!IsBlockLocked(entry)) {
      dir.erase(entry->addr);
      delete entry;
      entry = nullptr;
    } else {
      entry->state = DIR_UNSHARED;
    }
  }
}

void Directory::Clear(ptr_t ptr, GAddr addr) {
  epicAssert((ptr_t)ptr == TOBLOCK(ptr));
  ptr_t block = TOBLOCK(ptr);
  DirEntry* entry = GetEntry(block);
  epicAssert(!InTransitionState(entry));
  if (entry) {
    Clear(entry, addr);
  } else {
    epicLog(LOG_WARNING,
        "there must be a race before, and it has already been invalidated (%lx:%lx)",
        ptr, addr);
  }
}
