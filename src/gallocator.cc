// Copyright (c) 2018 The GAM Authors 

#include <stdio.h>
#include "gallocator.h"
#include "workrequest.h"
#include "zmalloc.h"
#include <cstring>

const Conf* GAllocFactory::conf = nullptr;
Worker* GAllocFactory::worker;
Master* GAllocFactory::master;
mutex GAllocFactory::lock;

GAddr GAlloc::Malloc(const Size size, Flag flag){
	GAddr addr = 0;
    while (1){
        this->txBegin();
	    addr = this->txAlloc(size);
	    int ret = this->txCommit();
        if (ret == 0) break;
    }
    return addr;
}

GAddr GAlloc::AlignedMalloc(const Size size, Flag flag){
    return this->Malloc(size, flag);
}

int GAlloc::Read(const GAddr addr, void* buf, const Size count, Flag flag){
	while (1){
        this->txBegin();
	    this->txRead(addr, reinterpret_cast<char*>(buf), count);
	    int ret = this->txCommit();
        if (ret == 0) break;
    }
    return 0;
}

int GAlloc::Read(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag){
	while (1){
        this->txBegin();
	    this->farm->txPartialRead(addr, offset, reinterpret_cast<char*>(buf), count);
	    int ret = this->txCommit();
        if (ret == 0) break;
    }
    return 0;
}
int GAlloc::Write(const GAddr addr, void* buf, const Size count, Flag flag){
	while (1){
        this->txBegin();
	    this->txWrite(addr, reinterpret_cast<char*>(buf), count);
	    int ret = this->txCommit();
        if (ret == 0) break;
    }
    return 0;
}
int GAlloc::Write(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag){
	while (1){
        this->txBegin();
	    this->farm->txPartialWrite(addr, offset, reinterpret_cast<char*>(buf), count);
	    int ret = this->txCommit();
        if (ret == 0) break;
    }
    return 0;
}
void GAlloc::Free(const GAddr addr){
	while (1){
        this->txBegin();
	    this->txFree(addr);
	    int ret = this->txCommit();
        if (ret == 0) break;
    }
}

void GAlloc::txBegin(){
	farm->txBegin();
}
GAddr GAlloc::txAlloc(size_t size, GAddr a){
	return farm->txAlloc(size, a);
}
void GAlloc::txFree(GAddr addr){
	farm->txFree(addr);
}
int GAlloc::txRead(GAddr addr, void* ptr, osize_t sz){
	return farm->txRead(addr, reinterpret_cast<char*>(ptr), sz);
}
int GAlloc::txRead(GAddr addr, const Size offset, void* ptr, osize_t sz){
    return farm->txPartialRead(addr, offset, reinterpret_cast<char*>(ptr), sz);
}
int GAlloc::txWrite(GAddr addr, void* ptr, osize_t sz){
	return farm->txWrite(addr, reinterpret_cast<char*>(ptr), sz);
}
int GAlloc::txWrite(GAddr addr, const Size offset, void* ptr, osize_t sz){
    return farm->txPartialWrite(addr, offset, reinterpret_cast<char*>(ptr), sz);
}
int GAlloc::txAbort(){
    return farm->txAbort();
}
int GAlloc::txCommit(){
	return farm->txCommit();
}

Size GAlloc::Put(uint64_t key, const void* value, Size count) {
    return farm->put(key, value, count);
}

Size GAlloc::Get(uint64_t key, void* value) {
    return farm->get(key, value);
}

int GAlloc::txKVGet(uint64_t key, void* value, int node_id){
	return farm->kv_get(key, value, node_id) < 0;
}
int GAlloc::txKVPut(uint64_t key, const void* value, size_t count, int node_id){
	return farm->kv_put(key, value, count, node_id) < 0;
}

int GAlloc::KVGet(uint64_t key, void *value, int node_id){
	return farm->kv_get(key, value, node_id) < 0;
}
int GAlloc::KVPut(uint64_t key, const void* value, size_t count, int node_id){
	return farm->kv_put(key, value, count, node_id) < 0;
}

