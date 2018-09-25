// Copyright (c) 2018 The GAM Authors 

#ifndef FARM_H_
#define FARM_H_ 

#include "worker.h"
#include "worker_handle.h"
#include "farm_txn.h"

class Farm {
    private:
        std::unique_ptr<WorkerHandle> wh_;
        std::unique_ptr<TxnContext> rtx_;
        TxnContext* tx_;
        Worker* w_;

    public:
        Farm(Worker*);
        int txBegin();
        GAddr txAlloc(size_t size, GAddr a = 0);
        void txFree(GAddr);
        osize_t txRead(GAddr, char*, osize_t);
        osize_t txWrite(GAddr, const char*, osize_t); 
        osize_t txPartialRead(GAddr, osize_t, char*, osize_t);
        osize_t txPartialWrite(GAddr, osize_t, const char*, osize_t);
        int txCommit();
        int txAbort(); 

        bool txnIsLocal() {
            std::vector<uint16_t> wid, rid;
            tx_->getWidForRobj(rid);
            tx_->getWidForWobj(wid);
            uint16_t wr = w_->GetWorkerId();
            bool ret = ((wid.size() == 0 || (wid.size() == 1 && wid[0] == wr))
                        && (rid.size() == 0 || (rid.size() == 1 && rid[0] == wr)));
            return ret;
        }

        int put(uint64_t key, const void* value, size_t count) ;
        int get(uint64_t key, void* value) ;
        int kv_put(uint64_t key, const void* value, size_t count, int node_id) ;
        int kv_get(uint64_t key, void* value, int node_id) ;
};
#endif
