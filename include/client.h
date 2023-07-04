// Copyright (c) 2018 The GAM Authors

#ifndef CLIENT_H
#define CLIENT_H

#include <mutex>

#include "lockwrapper.h"
#include "rdma.h"
#include "structure.h"

class Server;

// TODO: consider to replace Client by RdmaContext
class Client
{
private:
  RdmaContext *ctx; /* remote client */

  RdmaResource *resource;
  char *connstr = nullptr;

  // remote worker info
  /*
   * when connected to master, wid is its own wid
   * otherwise, it's the id of the remote pair
   */
  int wid = 0;
  Size size = 0;
  Size free = 0;

  LockWrapper lock_;

public:
  Client(RdmaResource *res, bool isForMaster,
         const char *rdmaConnStr = nullptr);

  // used only among workers
  int ExchConnParam(const char *ip, int port, Server *server);
  const char *GetConnString(int workerid = 0);
  int SetRemoteConnParam(const char *conn);

  inline bool IsForMaster()
  {
    return ctx->IsMaster();
  }
  inline int GetWorkerId()
  {
    return wid;
  }
  inline void SetMemStat(int size, int free)
  {
    this->size = size;
    this->free = free;
  }
  inline Size GetFreeMem()
  {
    return this->free;
  }
  inline Size GetTotalMem()
  {
    return this->size;
  }
  inline void *ToLocal(GAddr addr)
  {
    return TO_LOCAL(addr, ctx->GetBase());
  }
  inline void *ToLocal_flush(GAddr addr)
  {
    epicLog(LOG_DEBUG, "workid=%d, addr=%lx,ctx->GetBase()=%lx", wid, addr, ctx->GetBase());
    return TO_LOCAL(addr, ctx->GetBase());
  }
  inline GAddr ToGlobal(void *ptr)
  {
    if (ptr)
    {
      return TO_GLOB(ptr, ctx->GetBase(), wid);
    }
    else
    {
      return EMPTY_GLOB(wid);
    }
  }

  inline uint32_t GetQP()
  {
    return ctx->GetQP();
  }

  inline ssize_t Send(const void *buf, size_t len, unsigned int id = 0,
                      bool signaled = false)
  {
    return ctx->Send(buf, len, id, signaled);
  }
  inline ssize_t Write(raddr dest, raddr src, size_t len, unsigned int id = 0,
                       bool signaled = false)
  {
    return ctx->Write(dest, src, len, id, signaled);
  }
  inline ssize_t WriteWithImm(raddr dest, raddr src, size_t len, uint32_t imm,
                              unsigned int id = 0, bool signaled = false)
  {
    return ctx->WriteWithImm(dest, src, len, imm, id, signaled);
  }

  inline int PostRecv(int n)
  {
    return ctx->PostRecv(n);
  }

  inline char *GetFreeSlot()
  {
    return ctx->GetFreeSlot();
  }
  inline char *RecvComp(ibv_wc &wc)
  {
    return ctx->RecvComp(wc);
  }
  inline unsigned int SendComp(ibv_wc &wc)
  {
    return ctx->SendComp(wc);
  }
  inline unsigned int WriteComp(ibv_wc &wc)
  {
    return ctx->WriteComp(wc);
  }

  // below are used for multithread
  inline void lock()
  {
    epicLog(LOG_INFO, "trying to lock client %d", GetWorkerId());
    lock_.lock();
    epicLog(LOG_INFO, "locked client %d", GetWorkerId());
  }
  inline void unlock()
  {
    lock_.unlock();
    epicLog(LOG_INFO, "unlock client %d", GetWorkerId());
  }

  ~Client();
};
#endif
