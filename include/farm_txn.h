// Copyright (c) 2018 The GAM Authors 

#ifndef FARM_TXN_H
#define FARM_TXN_H
#include <cstdint>
#include <string>
#include <unordered_map>
#include <memory>
#include <vector>

#include "structure.h"
#include "workrequest.h"
#include "chars.h"

typedef int32_t osize_t;

typedef uint64_t version_t;
#define VBITS 56
#define MAX_VERSION ((1UL << VBITS) - 1)
#define UNLOCK 0x0
#define RLOCK 0x1 
#define WLOCK 0x2
#define RWLOCK (RLOCK | WLOCK)

static inline void runlock_version(version_t* v) {
    version_t v1 = WLOCK;
    (*v) &= ((v1 << VBITS) | MAX_VERSION);
}

static inline void wunlock_version(version_t* v) {
    version_t v1 = RLOCK;
    (*v) &= ((v1 << VBITS) | MAX_VERSION);
}

static inline void unlock_version(version_t* v) {
    *v &= MAX_VERSION;
}

static inline void rlock_version(version_t* v) {
    version_t v1 = RLOCK;
    *v |= (v1 << VBITS);
}

static inline void wlock_version(version_t* v) {
    version_t v1 = WLOCK;
    *v |= (v1 << VBITS);
}

static inline bool is_version_rlocked(version_t v) {
    return (v >> VBITS) & RLOCK;
}

static inline bool is_version_wlocked(version_t v) {
    return (v >> VBITS) & WLOCK;
}

static inline bool is_version_locked(version_t v) {
    return is_version_rlocked(v) | is_version_wlocked(v);
}

static inline bool is_version_diff(version_t before, version_t after) {
    if (is_version_wlocked(before) || is_version_wlocked(after))
        return true;

    runlock_version(&before);
    runlock_version(&after);
    epicAssert(!is_version_locked(before) && !is_version_locked(after));
    if (before != after)
        return true;
    return false;
}

static inline bool rlock_object(void* object) {
    version_t ov, nv;
    ov = __atomic_load_n((version_t*)object,  __ATOMIC_RELAXED);
    if (is_version_locked(ov))
        // rlock an object only if it is free
        return false;
    nv = ov;
    rlock_version(&nv);
    return __atomic_compare_exchange_n((version_t*)object, &ov, nv, true,
            __ATOMIC_RELAXED, __ATOMIC_RELAXED);
}

static inline bool wlock_object(void *object) {
    version_t ov, nv;
    ov = __atomic_load_n((version_t*)object, __ATOMIC_RELAXED);
    epicAssert(is_version_rlocked(ov) && !is_version_wlocked(ov));
    nv = ov;
    runlock_version(&nv);
    if (++nv == MAX_VERSION) {
        nv == 1;
    }
    wlock_version(&nv);
    return __atomic_compare_exchange_n((version_t*)object, &ov, nv, true,
            __ATOMIC_RELAXED, __ATOMIC_RELAXED);
}

static inline void runlock_object(void* object) {
    version_t ov;
    ov = __atomic_load_n((version_t*)object, __ATOMIC_RELAXED);
    epicAssert(is_version_rlocked(ov) && !is_version_wlocked(ov));
    runlock_version(&ov);
    __atomic_store_n((version_t*)object, ov, __ATOMIC_RELAXED);
}

static inline void wunlock_object(void* object) {
    version_t ov;
    ov = __atomic_load_n((version_t*)object, __ATOMIC_RELAXED);
    epicAssert(is_version_wlocked(ov) && !is_version_rlocked(ov));
    wunlock_version(&ov);
    __atomic_store_n((version_t*)object, ov, __ATOMIC_RELEASE);
}

static inline bool is_object_rlocked(void* object) {
    version_t v = __atomic_load_n((version_t*)object, __ATOMIC_RELAXED);
    return is_version_rlocked(v);
}

static inline bool is_object_wlocked(void* object) {
    version_t v = __atomic_load_n((version_t*)object, __ATOMIC_ACQUIRE);
    return is_version_wlocked(v);
}

static inline bool is_object_locked(void* object) {
    version_t v = __atomic_load_n((version_t*)object, __ATOMIC_RELAXED);
    return is_version_locked(v);
}


//#define PREPARE 1
//#define VALIDATE 2
//#define ABORT 3
//#define COMMIT 4

class TxnContext;

class Object {
    /* object layout: |version|size|data| */
    private:
        GAddr addr_;
        version_t version_;
        uint32_t pos_;
        osize_t size_;
        std::string& buf_;

    public:
        Object(std::string&, GAddr); 

        osize_t deserialize(const char*, osize_t = -1);
        osize_t serialize(char*, osize_t);
        osize_t writeTo(char*, int = 0, osize_t = -1);
        osize_t readFrom(const char*);
        osize_t readEmPlace(const char*, osize_t, osize_t);

        inline GAddr getAddr() {return addr_;}

        inline void setVersion(version_t v) {this->version_ = v;}
        inline version_t getVersion() {return version_;}

        inline osize_t getSize() {return size_;}
        inline void setSize(osize_t sz) { size_ = sz; }

        inline osize_t getTotalSize() {
            if (size_ == -1)
                return sizeof(version_t) + sizeof(osize_t);
            else
                return size_ + sizeof(version_t) + sizeof(osize_t);
        }

        inline bool hasContent() {return (pos_ != -1);}
        inline void freeContent() { pos_ = -1; }

        const char* toString();
        //inline void unlock() {unlock_version(&this->version_);}
        //inline void lock() {lock_version(&this->version_);}
        //inline bool isLocked() {return is_version_locked(this->version_);}
};

class TxnContext{
    private:
        std::unordered_map<uint16_t, std::unordered_map<GAddr, std::shared_ptr<Object>>> write_set_;
        std::unordered_map<uint16_t, std::unordered_map<GAddr, std::shared_ptr<Object>>> read_set_;
        std::string buffer_;


    public:
        WorkRequest* wr_;

        TxnContext(){wr_ = new WorkRequest;}
        ~TxnContext() {delete wr_;}

        inline std::string& getBuffer() {return this->buffer_;}

        Object* getReadableObject(GAddr); // {return read_set_.at(a>>48).at(a).get();}
        Object* createNewWritableObject(GAddr);
        Object* createReadableObject(GAddr);
        Object* createWritableObject(GAddr);
        Object* getWritableObject(GAddr);
        inline bool containWritable(GAddr a) {
            return this->write_set_[WID(a)].count(a) > 0;
        }

        inline void rmReadableObject(GAddr a) {
            this->read_set_[WID(a)].erase(a);
        }

        int generatePrepareMsg(uint16_t wid, char* msg, int len, int& nobj );
        int generateValidateMsg(uint16_t wid, char* msg, int len, int& nobj ); 
        int generateCommitMsg(uint16_t wid, char* msg, int len);
        int generateAbortMsg(uint16_t wid, char* msg, int len);

        void getWidForRobj(std::vector<uint16_t>& wid);
        void getWidForWobj(std::vector<uint16_t>& wid);

        inline int getNumWobjForWid(uint16_t w) {
            //return write_set_.count(w) > 0 ? write_set_.at(w).size() : 0;
            return write_set_.count(w) > 0 ? write_set_.at(w).size() : 0;
        }

        inline int getNumRobjForWid(uint16_t w) {
            return read_set_.count(w) > 0 ? read_set_.at(w).size(): 0;
        }

        inline std::unordered_map<GAddr, std::shared_ptr<Object>>& getReadSet(uint16_t wid) {
            epicAssert(read_set_.count(wid) == 1);
            return read_set_.at(wid);
        }

        inline std::unordered_map<GAddr, std::shared_ptr<Object>>& getWriteSet(uint16_t wid) {
            epicAssert(write_set_.count(wid) == 1);
            return write_set_.at(wid);
        }

        void reset();

};

#endif
