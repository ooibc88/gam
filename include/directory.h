// Copyright (c) 2018 The GAM Authors

#ifndef INCLUDE_DIRECTORY_H_
#define INCLUDE_DIRECTORY_H_

#include <list>
#include <unordered_map>
#include "settings.h"
#include "hashtable.h"
#include "map.h"
#ifdef B_I
#include "util.h"
#endif

#define NUM_SUBBLOCK 3

enum DirState
{
  DIR_UNSHARED,
  DIR_DIRTY,
  DIR_SHARED,
  DIR_TO_DIRTY,
  DIR_TO_SHARED,
  DIR_TO_UNSHARED
};

#ifdef B_I
struct BI_dir{
  uint64 Timestamp;
  list<GAddr> shared;
};
#endif

struct DirEntry
{
  DirState state = DIR_UNSHARED;
  list<GAddr> shared;
  ptr_t addr;
  /* add ergeda add */
  DataState Dstate = DataState::MSI;
  GAddr owner;
  /* add ergeda add */
  /* add wpq add */
  int ownerlist_subblock[NUM_SUBBLOCK] = {1, 2, 3};
  int ownerNumber = 3;
  int linelist_subblock[NUM_SUBBLOCK] = {200, 300, 512};
  /* add wpq add */
  // if lock == 0, no one is holding the lock. otherwise, there are #lock ones holding the lock
  // but if lock = EXCLUSIVE_LOCK_TAG, it is a exclusive lock
  // int lock = 0;
  unordered_map<ptr_t, int> locks;
  #ifdef SUB_BLOCK
  //std::vector <DirEntry *> SubEntry; //实际上子目录似乎只需要知道每个块大小情况即可，每个子块就会建一个目录，所以也不需要记录子目录位置。
  int MySize;
  #endif

  #ifdef DYNAMIC
  uint64 Race_time = 0; //统计写写冲突发生的次数
  std::unordered_map <uint64, uint64> Left; //统计左半边访问的情况
  std::unordered_map <uint64, uint64> Right;//统计右半边访问的情况
  int MetaVersion = 0; //记录目录的版本信息
  #endif

  #ifdef B_I
  list<BI_dir *> version_list; // 版本链条
  #endif
};

class Directory
{
  /*
   * the directory structure
   * key: BLOCK-aligned address
   * value: directory entry
   */
  // unordered_map<ptr_t, DirEntry> dir;
#ifdef USE_SIMPLE_MAP
  Map<ptr_t, DirEntry *> dir{"directory"};
#else
  HashTable<ptr_t, DirEntry *> dir{"directory"};
#endif

  DirState GetState(ptr_t ptr);
  bool InTransitionState(ptr_t ptr);
  int RLock(ptr_t ptr);
  int RLock(DirEntry *entry, ptr_t ptr);
  bool IsRLocked(ptr_t ptr);
  bool IsRLocked(
      DirEntry *entry, ptr_t ptr);
  int WLock(ptr_t ptr);
  int WLock(DirEntry *entry, ptr_t ptr);
  bool IsWLocked(ptr_t ptr);
  bool IsWLocked(
      DirEntry *entry, ptr_t ptr);

  DirEntry *GetEntry(ptr_t ptr)
  {
#ifdef SUB_BLOCK
    if (dir.count(ptr)) return dir.at(ptr);
    else return nullptr;
#endif
    if (dir.count(TOBLOCK(ptr)))
    {
      return dir.at(TOBLOCK(ptr));
    }
    else
    {
      return nullptr;
    }
  }

  void UnLock(ptr_t ptr);
  void UnLock(DirEntry *&entry, ptr_t ptr);
  bool IsBlockWLocked(ptr_t block);
  bool IsBlockLocked(
      ptr_t block);
  void Clear(ptr_t ptr, GAddr addr);
  inline list<GAddr> &GetSList(ptr_t ptr)
  {
#ifdef SUB_BLOCK
    return dir.at(ptr)->shared;
#endif
    return dir.at(TOBLOCK(ptr))->shared;
  }
  inline void lock(ptr_t ptr)
  {
    epicAssert(BLOCK_ALIGNED(ptr));
    LOCK_MICRO(dir, ptr);
  }
  inline void unlock(ptr_t ptr)
  {
    epicAssert(BLOCK_ALIGNED(ptr));
    UNLOCK_MICRO(dir, ptr);
  }

public:
  /*
   * @ptr is the local virtual address
   */
  inline DirState GetState(void *ptr)
  {
    return GetState((ptr_t)ptr);
  }
  inline DirState GetState(DirEntry *entry)
  {
    if (!entry)
    {
      return DIR_UNSHARED;
    }
    else
    {
      return entry->state;
    }
  }
  inline bool InTransitionState(void *ptr)
  {
    return InTransitionState((ptr_t)ptr);
  }
  inline bool InTransitionState(DirState s)
  {
    return s == DIR_TO_DIRTY || s == DIR_TO_SHARED || s == DIR_TO_UNSHARED;
  }
  inline bool InTransitionState(DirEntry *entry)
  {
    if (!entry)
    {
      return false;
    }
    else
    {
      return InTransitionState(entry->state);
    }
  }
  DirEntry *GetEntry(void *ptr)
  {
    return GetEntry((ptr_t)ptr);
  }

  
#ifdef SUB_BLOCK
  DirEntry* GetSubEntry(ptr_t ptr) {
    if (dir.count(ptr)) {
      return dir.at(ptr);
    } else {
      return nullptr;
    }
  }

  DirEntry * GetSubEntry(void * ptr) {
    return GetSubEntry((ptr_t) ptr);
  }
#endif

#ifdef B_I
  BI_dir * Create_BIdir () {
    BI_dir * BI_entry = new BI_dir();
    BI_entry->Timestamp = get_time();
    return BI_entry;
  }

  void Add_BIdir (DirEntry * Entry, BI_dir * BI_entry) {
    Entry->version_list.push_back(BI_entry);
  }

  void Delete_BIdirbegin (DirEntry * Entry) {
    //epicLog(LOG_WARNING, "got delete birdir");
    auto it = Entry->version_list.begin();
    BI_dir * BI_entry = (*it);
    Entry->version_list.erase(it);
    BI_entry->shared.clear();
    delete BI_entry;
    BI_entry = nullptr;
  }

  BI_dir * getlastbientry(DirEntry * Entry) {
    return Entry->version_list.back();
  }

  uint64 getlastversion(DirEntry * Entry) {
    if (Entry->version_list.empty()) return 0;
    return (Entry->version_list.back())->Timestamp;
  }
#endif

  /* add ergeda add */

  /*  MetaEntry* GetMeta(GAddr addr) {
      if (Meta.count(addr)) {
        return Meta.at(addr);
      }
      else return nullptr;
    }
  */
  DataState GetDataState(DirEntry *Entry)
  {
    if (Entry == nullptr)
    {
      // epicLog(LOG_WARNING, "MetaEntry == nullptr\n");
      return DataState::MSI;
    }
    return Entry->Dstate;
  }

  /* add wpq add*/
  inline int GetSubBlockNum(DirEntry *Entry, GAddr addr)
  {
    // linelist_subblock
    int offset = addr - TOBLOCK(addr);

    for (int i = 0; i < NUM_SUBBLOCK; i++)
    {
      if (offset < Entry->linelist_subblock[i])
      {
        return i;
      }
    }
  }

  inline int GetSubBlockOwner(DirEntry *Entry, int subblock)
  {
    return Entry->ownerlist_subblock[subblock];
  }
  inline GAddr GetSubBlockAddr(DirEntry *Entry, int subblock_owner)
  {
    return EMPTY_GLOB(subblock_owner);
  }

  /* add wpq add*/

  DataState GetDataState(GAddr addr)
  {
    return GetDataState(GetEntry(addr));
  }

  GAddr GetOwner(DirEntry *Entry)
  {
    return Entry->owner;
  }

  GAddr GetOwner(GAddr addr)
  {
    return GetOwner(GetEntry(addr));
  } /*
   void CreateMetaEntry(GAddr addr, DataState Dstate=DataState::MSI, GAddr Owner=1) {
     MetaEntry * Entry = GetMeta(addr);
     if (Entry == nullptr) {
       Entry = new MetaEntry();
       Entry->Dstate = Dstate;
       Entry->Last_def = 0;
       Entry->owner = Owner;
       Meta[addr] = Entry;
     }
     else {
       epicLog(LOG_WARNING, "meta already exist\n");
     }
   }
 */
  void SetDataState(DirEntry *Entry, DataState Dstate)
  {
    Entry->Dstate = Dstate;
  }

  void SetMetaOwner(DirEntry *Entry, GAddr Owner)
  {
    Entry->owner = Owner;
  }

  void CreateEntry(void *ptr, DataState Cur_state = DataState::MSI, GAddr Owner = 1)
  {
#ifdef SUB_BLOCK
    ptr_t block = (ptr_t)ptr;
#else
    ptr_t block = TOBLOCK(ptr);
#endif
    DirEntry *entry = GetEntry(ptr);
    if (entry == nullptr)
    {
      entry = new DirEntry();
      entry->state = DIR_UNSHARED;
      entry->Dstate = Cur_state;
      entry->addr = block;
      entry->owner = Owner;
      dir[block] = entry;
#ifdef DYNAMIC
      entry->MetaVersion = 1;
#endif

#ifdef B_I
      if (Cur_state == DataState::BI) {
        BI_dir * cur_bientry = Create_BIdir();
        Add_BIdir(entry, cur_bientry); //最开始建立一个版本
      }
#endif
    }
#ifdef SUB_BLOCK
    if (Cur_state == WRITE_SHARED) {
      int Divide = 2;
      int CurSize = (BLOCK_SIZE / Divide);

      entry->MySize = CurSize;

      ptr_t CurStart = block;
      for (int i = 0; i < Divide - 1; ++i) {
        CurStart += CurSize;
        DirEntry * CurEntry = new DirEntry();
        CurEntry->state = DIR_UNSHARED;
        CurEntry->Dstate = Cur_state;
        CurEntry->addr = CurStart;
        CurEntry->owner = Owner;
        dir[CurStart] = CurEntry;
        CurEntry->MySize = CurSize;
      }
    }
    else entry->MySize = BLOCK_SIZE;
#endif    
  }

  void SetShared(DirEntry *Entry)
  {
    Entry->state = DIR_SHARED;
  }

  void SetDirty(DirEntry *Entry)
  {
    Entry->state = DIR_DIRTY;
  }

  void SetUnshared(DirEntry *Entry)
  {
    Entry->state = DIR_UNSHARED;
  }

#ifdef DYNAMIC
  void DirInit(DirEntry * Entry, DataState Curs=DataState::MSI, int CurSize = BLOCK_SIZE, int CurVersion = 1) {
    Entry->Dstate = Curs;
    Entry->MySize = CurSize;
    Entry->MetaVersion = CurVersion;
    Entry->state = DIR_UNSHARED;
    Entry->Left.clear();
    Entry->Right.clear();
    Entry->Race_time = 0;
    Entry->owner = 0;
    Entry->shared.clear();
  }

  uint64 GetRacetime (DirEntry * entry) {
    if (entry == nullptr) {
      epicLog(LOG_WARNING, "no entry when asking racetime");
      return 0;
    }
    return entry->Race_time;
  }

  uint64 GetRacetime(void * ptr_t) {
    return GetRacetime(GetEntry(ptr_t));
  }

  int GetVersion (DirEntry * entry) {
    if (entry == nullptr) {
      epicLog (LOG_WARNING, "no entry when asking version");
      return 0;
    }
    return entry->MetaVersion;
  }
  
  int GetVersion (void * ptr_t) {
    return GetVersion(GetEntry(ptr_t));
  }
#endif
  /* add ergeda add */

  DirEntry *ToShared(void *ptr, GAddr addr);
  void ToShared(DirEntry *entry, GAddr addr);
  DirEntry *ToDirty(void *ptr, GAddr addr);
  void ToDirty(DirEntry *entry, GAddr addr);
  void ToUnShared(void *ptr);
  void ToUnShared(DirEntry *&entry);
  void ToToShared(void *ptr, GAddr addr = Gnullptr);
  void ToToShared(DirEntry *entry, GAddr addr = Gnullptr);
  DirEntry *ToToDirty(void *ptr, GAddr addr = Gnullptr);
  inline void ToToDirty(DirEntry *entry, GAddr = Gnullptr)
  {
    epicAssert(entry);
    entry->state = DIR_TO_DIRTY;
  }
  void ToToUnShared(void *ptr);
  void ToToUnShared(DirEntry *entry);
  void UndoDirty(void *ptr);
  void UndoDirty(DirEntry *entry);
  void UndoShared(void *ptr);
  void UndoShared(DirEntry *entry);
  void Remove(void *ptr, int wid);
  void Remove(DirEntry *&entry, int wid);

  inline int RLock(void *ptr)
  {
    return RLock((ptr_t)ptr);
  }
  inline int RLock(DirEntry *entry, void *ptr)
  {
    return RLock(entry, (ptr_t)ptr);
  }
  int RLock(DirEntry *entry) = delete;

  inline bool IsRLocked(void *ptr)
  {
    return IsRLocked((ptr_t)ptr);
  }
  inline bool IsRLocked(DirEntry *entry, void *ptr)
  {
    return IsRLocked(entry, (ptr_t)ptr);
  }
  bool IsRLocked(DirEntry *entry) = delete;

  inline int WLock(void *ptr)
  {
    return WLock((ptr_t)ptr);
  }
  inline int WLock(DirEntry *entry, void *ptr)
  {
    return WLock(entry, (ptr_t)ptr);
  }
  int WLock(DirEntry *entry) = delete;

  inline bool IsWLocked(void *ptr)
  {
    return IsWLocked((ptr_t)ptr);
  }
  inline bool IsWLocked(DirEntry *entry, void *ptr)
  {
    return IsWLocked(entry, (ptr_t)ptr);
  }
  bool IsWLocked(DirEntry *entry) = delete;

  inline void UnLock(void *ptr)
  {
    UnLock((ptr_t)ptr);
  }
  inline void UnLock(DirEntry *&entry, void *ptr)
  {
    UnLock(entry, (ptr_t)ptr);
  }
  void UnLock(DirEntry *entry) = delete;

  inline bool IsBlockWLocked(void *block)
  {
    return IsBlockWLocked((ptr_t)block);
  }
  bool IsBlockWLocked(DirEntry *entry);
  inline bool IsBlockLocked(void *block)
  {
    return IsBlockLocked((ptr_t)block);
  }
  bool IsBlockLocked(DirEntry *entry);

  inline void Clear(void *ptr, GAddr addr)
  {
    Clear((ptr_t)ptr, addr);
  }
  void Clear(DirEntry *&entry, GAddr addr);

  inline list<GAddr> &GetSList(void *ptr)
  {
    return GetSList((ptr_t)ptr);
  }
  list<GAddr> &GetSList(DirEntry *entry)
  {
    return entry->shared;
  }

  // below are used for multithread programming
  inline void lock(void *ptr)
  {
    lock((ptr_t)ptr);
  }
  inline void unlock(void *ptr)
  {
    unlock((ptr_t)ptr);
  }
  void unlock(DirEntry *entry) = delete;
  void lock(DirEntry *entry) = delete;
};

#endif /* INCLUDE_DIRECTORY_H_ */
