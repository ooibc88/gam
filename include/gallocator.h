// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_GALLOCATOR_H_
#define INCLUDE_GALLOCATOR_H_

#include <cstring>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "farm.h"

class GAlloc{
// the only class the app-layer is aware of
public:
	GAddr Malloc(const Size size, Flag flag = 0);
	GAddr AlignedMalloc(const Size size, Flag flag = 0);
    int Read(const GAddr addr, void* buf, const Size count, Flag flag = 0);
	int Read(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag = 0);
	int Write(const GAddr addr, void* buf, const Size count, Flag flag = 0);
	int Write(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag = 0);
	void Free(const GAddr addr);

	Size Put(uint64_t key, const void* value, Size count);
	Size Get(uint64_t key, void* value);

	void txBegin();
    GAddr txAlloc(size_t size, GAddr a = 0);
    void txFree(GAddr);
    int txRead(GAddr, void*, osize_t);
    int txRead(GAddr, const Size, void*, osize_t);
    int txWrite(GAddr, void*, osize_t); 
    int txWrite(GAddr, const Size, void*, osize_t);
    int txAbort();
    int txCommit();

   	int txKVGet(uint64_t key, void* value, int node_id);
   	int txKVPut(uint64_t key, const void* value, size_t count, int node_id);
   	int KVGet(uint64_t key, void *value, int node_id);
   	int KVPut(uint64_t key, const void* value, size_t count, int node_id);

    GAlloc(Worker* w) {
        farm = new Farm(w);
    }

    ~GAlloc() {delete farm;}

protected:
	Farm *farm;
};

class GAllocFactory {
	static const Conf* conf;
	static Worker* worker;
	static Master* master;
	static mutex lock;
public:
	/*
	 * this function should be call in every thread
	 * in order to init some per-thread data
	 */
	static GAlloc* CreateAllocator(const std::string& conf_file) {
		return CreateAllocator(ParseConf(conf_file));
	}

	static const Conf* InitConf() {
		lock.lock();
		conf = new Conf();
		const Conf* ret = conf;
		lock.unlock();
		return ret;
	}

	//need to call for every thread
	static GAlloc* CreateAllocator(const Conf* c = nullptr) {
		lock.lock();
		if(c) {
			if(!conf) {
				conf = c;
			} else {
				epicLog(LOG_INFO, "NOTICE: Conf already exist %lx", conf);
			}
		} else {
			if(!conf) {
				epicLog(LOG_FATAL, "Must provide conf for the first time");
			}
		}

		if(conf->is_master) {
			if(!master) master = MasterFactory::CreateServer(*conf);
		}
		if(!worker) {
			worker = WorkerFactory::CreateServer(*conf);
		}
		GAlloc* ret = new GAlloc(worker);
		lock.unlock();
		return ret;
	}

	//need to call for every thread
	static GAlloc* CreateAllocator(const Conf& c) {
		lock.lock();
		if(!conf) {
			Conf* lc = new Conf();
			*lc = c;
			conf = lc;
		} else {
			epicLog(LOG_INFO, "Conf already exist %lx", conf);
		}
		if(conf->is_master) {
			if(!master) master = MasterFactory::CreateServer(*conf);
		}
		if(!worker) {
			worker = WorkerFactory::CreateServer(*conf);
		}
		GAlloc* ret = new GAlloc(worker);
		lock.unlock();
		return ret;
	}

	static void SetConf(Conf* c) {
		lock.lock();
		conf = c;
		lock.unlock();
	}

	static void FreeResouce() {
		lock.lock();
		delete conf;
		delete worker;
		delete master;
		lock.unlock();
	}

	/*
	 * TODO: fake parseconf function
	 */
	static const Conf* ParseConf(const std::string& conf_file) {
		Conf* c = new Conf();
		return c;
	}

	static int LogLevel() {
		int ret = conf->loglevel;
		return ret;
	}

	static string* LogFile() {
		string* ret = conf->logfile;
		return ret;
	}
};

#endif /* INCLUDE_GALLOCATOR_H_ */
