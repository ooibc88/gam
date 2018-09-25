// Copyright (c) 2018 The GAM Authors 

#ifndef RDMA_H_
#define RDMA_H_

#include <map>
#include <unordered_map>
#include <cstdint>
#include <vector>
#include <cstdio>
#include <infiniband/verbs.h>
#include <queue>
#include <atomic>
#include <mutex>

#include "lockwrapper.h"
#include "settings.h"
#include "log.h"

class RdmaResource;
class RdmaContext;

typedef void* raddr;  //raddr means registered addr

#define MASTER_RDMA_CONN_STRLEN 22 /** 4 bytes for lid + 8 bytes qpn
                                    * + 8 bytes for psn
                                    * seperated by 2 colons */

#define WORKER_RDMA_CONN_STRLEN (MASTER_RDMA_CONN_STRLEN + 8 + 16 + 2)

#define MAX_CONN_STRLEN (WORKER_RDMA_CONN_STRLEN+4+1) //4: four-digital wid, 1: ':', 1: \0

#define HALF_BITS 0xffffffff
#define QUARTER_BITS 0xffff

#define RECV_SLOT_STEP 100
#define BPOS(s) (s/RECV_SLOT_STEP)
#define BOFF(s) ((s%RECV_SLOT_STEP)*MAX_REQUEST_SIZE)
#define RMINUS(a, b, rz) ((a) >= (b) ? (a) - (b) : (a) + (rz) - (b))

struct RdmaConnection { /* information of IB conn */
  uint64_t vaddr; /* buffer address */
  uint32_t qpn; /* QP number */
  uint32_t psn; /* packet sequence number */
  uint32_t rkey; /* remote key */
  uint32_t lid; /* LID of the IB port */
};

class RdmaResource {
  ibv_device *device;
  ibv_context *context;
  ibv_comp_channel *channel;
  ibv_pd *pd;
  ibv_cq *cq; /* global comp queue */
  ibv_srq *srq; /* share receive queue */
  const char *devName = NULL;
  ibv_port_attr portAttribute;
  int ibport = 1; /* TODO: dual-port support */
  uint32_t psn;bool isForMaster;
  friend class RdmaContext;

  //the follow three variables are only used for comm among workers
  void* base;  //the base addr for the local memory
  size_t size;
  struct ibv_mr *bmr;

  //node-wide communication buf used for receive request
  std::vector<struct ibv_mr*> comm_buf;
  //size_t buf_size; buf_size = slots.size() * MAX_REQUEST_SIZE
  int slot_head;  //current slot head
  int slot_inuse;  //number of slots in use
  /*
   * TODO: check whether head + tail is enough
   */
  std::vector<bool> slots;  //the states of all the allocated slots (true: occupied, false: free)
  //current created RdmaContext
  int rdma_context_counter;

  std::atomic<int> recv_posted;
  int rx_depth = 0;

 public:
  /*
   * isForMaster: true -- used to communicate with/by Master (no need to reg whole mem space)
   * 				false -- used to communicate among workers (reg whole mem space + comm buf)
   */
  RdmaResource(ibv_device *device, bool isForMaster);
  ~RdmaResource();
  inline const char *GetDevname() const {
    return this->devName;
  }
  inline bool IsMaster() const {
    return isForMaster;
  }
  //int regHtableMemory(const void *htable, size_t size);

  inline ibv_cq* GetCompQueue() const {
    return cq;
  }
  inline int GetChannelFd() const {
    return channel->fd;
  }
  bool GetCompEvent() const;
  int RegLocalMemory(void *base, size_t sz);

  int RegCommSlot(int);
  char* GetSlot(int s);  //get the starting addr of the slot
  int PostRecv(int n = 1);  //post n RR to the srq
  int PostRecvSlot(int slot);
  inline void ClearSlot(int s) {
    slots.at(s) = false;
  }

  RdmaContext* NewRdmaContext(bool isForMaster);
  void DeleteRdmaContext(RdmaContext* ctx);
  inline int GetCounter() {
    return rdma_context_counter;
  }

};

struct RdmaRequest {
  ibv_wr_opcode op;
  const void* src;
  size_t len;
  unsigned int id;bool signaled;
  void* dest;
  uint32_t imm;
  uint64_t oldval;
  uint64_t newval;
};

class RdmaContext {
 private:
  ibv_qp *qp;
  ibv_mr* send_buf;  //send buf
  int slot_head;
  int slot_tail;bool full;  //to differentiate between all free and all occupied slot_head == slot_tail

  uint64_t vaddr = 0; /* for remote rdma read/write */
  uint32_t rkey = 0;

  RdmaResource *resource;bool isForMaster;
  int max_pending_msg;
  int max_unsignaled_msg;
  atomic<int> pending_msg;  //including both RDMA send and write/read that don't use the send buf
  atomic<int> pending_send_msg;  //including only send msg
  atomic<int> to_signaled_send_msg;  //in order to proceed the slot_tail
  atomic<int> to_signaled_w_r_msg;
  queue<RdmaRequest> pending_requests;

  char *msg;

  ssize_t Rdma(ibv_wr_opcode op, const void* src, size_t len, unsigned int id =
                   0,
               bool signaled =
               false,
               void* dest = nullptr, uint32_t imm = 0, uint64_t oldval = 0,
               uint64_t newval = 0);

  inline ssize_t Rdma(RdmaRequest& r) {
    epicLog(LOG_DEBUG, "process pending rdma request");
    epicLog(
        LOG_DEBUG,
        "op = %d, src = %lx, len = %d, id = %d, signaled = %d, dest = %lx, imm = %u, oldval = %lu, newval = %lu\n"
        "src = %s",
        r.op, r.src, r.len, r.id, r.signaled, r.dest, r.imm, r.oldval, r.newval,
        r.src);
    return Rdma(r.op, r.src, r.len, r.id, r.signaled, r.dest, r.imm, r.oldval,
                r.newval);
  }

//for multithread
  LockWrapper global_lock_;
  inline void lock() {
    global_lock_.lock();
  }
  inline void unlock() {
    global_lock_.unlock();
  }
  char* GetFreeSlot_();

 public:
  RdmaContext(RdmaResource *, bool isForMaster);
  inline bool IsMaster() {
    return isForMaster;
  }
  const char* GetRdmaConnString();
  int SetRemoteConnParam(const char *remConn);

  inline uint32_t GetQP() {
    return qp->qp_num;
  }
  inline void* GetBase() {
    return (void*) vaddr;
  }

  unsigned int SendComp(ibv_wc& wc);
  unsigned int WriteComp(ibv_wc& wc);
  char* RecvComp(ibv_wc& wc);
  char* GetFreeSlot();bool IsRegistered(const void* addr);

  ssize_t Send(const void* ptr, size_t len, unsigned int id = 0, bool signaled =
                   false);
  inline int PostRecv(int n) {
    return resource->PostRecv(n);
  }
  int Recv();

  void ProcessPendingRequests(int n);

  /*
   * @dest: dest addr at remote node
   * @src: src addr at local node
   */
  ssize_t Write(raddr dest, raddr src, size_t len, unsigned int id = 0,
                bool signaled = false);
  ssize_t WriteWithImm(raddr dest, raddr src, size_t len, uint32_t imm,
                       unsigned int id = 0, bool signaled = false);
  /*
   * @dest: dest addr at local node
   * @src: src addr at remote node
   */
  ssize_t Read(raddr dest, raddr src, size_t len, unsigned int id = 0,
               bool signaled = false);

  ssize_t Cas(raddr src, uint64_t oldval, uint64_t newval, unsigned int id = 0,
              bool signaled = false);

  ~RdmaContext();
};

class RdmaResourceFactory {
  /* TODO: thread safety */
 private:
  static std::vector<RdmaResource *> resources;
  static const char *defaultDevname;
  static RdmaResource* GetRdmaResource(bool isServer, const char *devName);
 public:
  inline static RdmaResource* getMasterRdmaResource(
      const char *devName = NULL) {
    return GetRdmaResource(true, devName);
  }
  inline static RdmaResource* getWorkerRdmaResource(
      const char *devName = NULL) {
    return GetRdmaResource(false, devName);
  }

  ~RdmaResourceFactory() {
    for (std::vector<RdmaResource *>::iterator i = resources.begin();
        i != resources.end(); ++i) {
      delete (*i);
    }
  }
};
#endif
