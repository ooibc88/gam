// Copyright (c) 2018 The GAM Authors 

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <climits>
#include <arpa/inet.h>

#include "rdma.h"
#include "settings.h"
#include "zmalloc.h"
#include "log.h"
#include "kernel.h"
#include "workrequest.h"

static int page_size = 4096;
int MAX_RDMA_INLINE_SIZE = 256;

RdmaResource::RdmaResource(ibv_device *dev, bool master)
    : device(dev),
      isForMaster(master),
      base(nullptr),
      bmr(nullptr),
      size(0),
      rdma_context_counter(0),
      slot_inuse(0),
      slot_head(0),
      recv_posted(0) {

  epicLog(LOG_DEBUG, "new rdma resource\n");

  if (!(context = ibv_open_device(dev))) {
    epicLog(LOG_FATAL, "unable to get context for %s\n",
            ibv_get_device_name(dev));
    return;
  }

  if (!(channel = ibv_create_comp_channel(this->context))) {
    epicLog(LOG_FATAL, "Unable to create comp channel\n");
    goto clean_ctx;
  }

  if (!(pd = ibv_alloc_pd(this->context))) {
    epicLog(LOG_FATAL, "Unable to allocate pd\n");
    goto clean_channel;
  }

  rx_depth =
      (isForMaster) ? MASTER_RDMA_SRQ_RX_DEPTH : WORKER_RDMA_SRQ_RX_DEPTH;
  rx_depth = rx_depth > HW_MAX_PENDING ? HW_MAX_PENDING : rx_depth;
  if (!(cq = ibv_create_cq(this->context, (rx_depth << 1) + 1, NULL,
                           this->channel, 0))) {
    epicLog(LOG_FATAL, "Unable to create cq\n");
    goto clean_pd;
  }

  {
    ibv_srq_init_attr attr = { };
    attr.attr.max_wr = rx_depth;
    attr.attr.max_sge = 1;

    if (!(srq = ibv_create_srq(this->pd, &attr))) {
      epicLog(LOG_FATAL, "Unable to create srq\n");
      goto clean_cq;
    }
  }

  if (ibv_query_port(context, ibport, &portAttribute)) {
    epicLog(LOG_FATAL, "Unable to query port %d\n", ibport);
    goto clean_srq;
  }

  devName = ibv_get_device_name(this->device);
  srand48(time(NULL));
  psn = lrand48() & 0xffffff;

#ifdef RDMA_POLL
  if (isForMaster) {
#endif
    /* Request notification upon the next completion event */
    if (ibv_req_notify_cq(cq, 0)) {
      fprintf(stderr, "Couldn't request CQ notification\n");
      return;
    }
#ifdef RDMA_POLL
  }
#endif

  return;

  clean_srq: ibv_destroy_srq(this->srq);
  clean_cq: ibv_destroy_cq(this->cq);
  clean_pd: ibv_dealloc_pd(this->pd);
  clean_channel: ibv_destroy_comp_channel(this->channel);
  clean_ctx: ibv_close_device(this->context);

  throw RDMA_RESOURCE_EXCEPTION;
}

RdmaResource::~RdmaResource() {
  ibv_destroy_srq(this->srq);
  ibv_destroy_cq(this->cq);
  ibv_dealloc_pd(this->pd);
  ibv_destroy_comp_channel(this->channel);
  ibv_close_device(this->context);
  for (ibv_mr* mr : comm_buf) {
    ibv_dereg_mr(mr);
    zfree(mr->addr);
  }
}

int RdmaResource::RegLocalMemory(void *base, size_t sz) {
  int ret = -1;
  if (this->base || this->bmr || isForMaster) {
    epicLog(LOG_WARNING, "An mr has already be registered or I am a master\n");
    return ret;
  }

  bmr = ibv_reg_mr(
      this->pd,
      const_cast<void *>(base),
      sz,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
          | IBV_ACCESS_REMOTE_READ);
  if (!bmr) {
    epicLog(LOG_FATAL, "Unable to register mr for hash table");
    return ret;
  }

  this->base = base;
  return 0;
}

int RdmaResource::RegCommSlot(int slot) {
  epicLog(LOG_DEBUG, "trying to register %d slots", slot);

  if (slots.size() - slot_inuse >= slot) {
    epicLog(LOG_DEBUG, "no need to register: inuse = %d, current slots = %d\n",
            slot_inuse, slots.size());
    slot_inuse += slot;
    return 0;
  } else {
    slot_inuse += slot;
    int i = slots.size();
    for (; i < slot_inuse; i += RECV_SLOT_STEP) {
      int sz = roundup(RECV_SLOT_STEP*MAX_REQUEST_SIZE, page_size);
      void* buf = zmalloc(sz);
      struct ibv_mr *mr = ibv_reg_mr(
          this->pd,
          buf,
          sz,
          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
              | IBV_ACCESS_REMOTE_READ);
      if (!mr) {
        epicLog(LOG_FATAL, "Unable to register mr for communication slots");
        return -1;
      }
      comm_buf.push_back(mr);
      epicAssert(mr->addr == buf && mr->length == sz);
    }
    slots.reserve(i);
    for (int j = slots.size(); j < i; j++) {
      slots.push_back(false);
    }

    epicLog(LOG_DEBUG, "registered %d, enlarge to %d with inuse = %d\n", slot,
            slots.size(), slot_inuse);
    epicAssert(slots.size() % RECV_SLOT_STEP == 0);
    return 0;
  }
}

char* RdmaResource::GetSlot(int slot) {
  epicAssert(slots.at(slot) == true && slot < slot_inuse);
  //TODO: check slot == tail
  return (char*) ((uintptr_t) comm_buf[BPOS(slot)]->addr + BOFF(slot));
}

int RdmaResource::PostRecvSlot(int slot) {
  if (rx_depth == recv_posted) {
    epicLog(LOG_WARNING, "cannot post any recv, already full");
    return 0;
  }

  recv_posted += 1;
  ibv_recv_wr rr { };
  ibv_sge sge { };
  int bpos = BPOS(slot);
  int boff = BOFF(slot);

  sge.length = MAX_REQUEST_SIZE;
  sge.lkey = comm_buf[bpos]->lkey;
  sge.addr = (uintptr_t) comm_buf[bpos]->addr + boff;

  rr.wr_id = slot;
  rr.num_sge = 1;
  rr.sg_list = &sge;
  rr.next = nullptr;

  ibv_recv_wr* bad_rr;
  if (ibv_post_srq_recv(srq, &rr, &bad_rr)) {
    epicLog(LOG_WARNING, "post recv request failed (%d:%s)\n", errno,
            strerror(errno));
    slots.at(slot) = false;
    return 0;
  }
  return 1;
}

int RdmaResource::PostRecv(int n) {
  if (n > rx_depth - recv_posted) {
    n = rx_depth - recv_posted;
  }
  epicAssert(n >= 0);
  if (n == 0)
    return 0;

  ibv_recv_wr rr[n];
  memset(rr, 0, sizeof(ibv_recv_wr) * n);
  ibv_sge sge[n];
  int i, ret;
  int head_init = slot_head;
  for (i = 0; i < n;) {
    if (slots.at(slot_head) == true) {
      if (++slot_head == slot_inuse)
        slot_head = 0;
      if (slot_head == head_init) {
        epicLog(LOG_FATAL, "cannot find free recv slot (%d)", n);
        break;
      }
      continue;
    }
    int bpos = BPOS(slot_head);
    int boff = BOFF(slot_head);

    sge[i].length = MAX_REQUEST_SIZE;
    sge[i].lkey = comm_buf[bpos]->lkey;
    sge[i].addr = (uintptr_t) comm_buf[bpos]->addr + boff;

    rr[i].wr_id = slot_head;
    rr[i].num_sge = 1;
    rr[i].sg_list = &sge[i];
    if (i + 1 < n)
      rr[i].next = &rr[i + 1];

    //advance the slot_head by 1
    slots.at(slot_head) = true;
    if (++slot_head == slot_inuse)
      slot_head = 0;
    i++;
  }
  ret = i;

  if (i > 0) {
    rr[i - 1].next = nullptr;
    ibv_recv_wr* bad_rr;
    if (ibv_post_srq_recv(srq, rr, &bad_rr)) {
      epicLog(LOG_WARNING, "post recv request failed (%d:%s)\n", errno,
              strerror(errno));
      int s = bad_rr->wr_id;
      ret -= RMINUS(slot_head, s, slot_inuse);
      while (s != slot_head) {
        slots.at(s) = false;
        if (++s == slot_inuse)
          s = 0;
      }
      slot_head = s;
    }
  }
  recv_posted += n;
  return ret;
}

const char *RdmaResourceFactory::defaultDevname = NULL;
std::vector<RdmaResource *> RdmaResourceFactory::resources;

RdmaResource* RdmaResourceFactory::GetRdmaResource(bool isMaster, const char *devName) {

  if (!devName) {
    devName = defaultDevname;
  }

  if (devName) {
    for (std::vector<RdmaResource *>::iterator it = resources.begin();
        it != resources.end(); ++it) {
      if (!strcmp((*it)->GetDevname(), devName)
          && (*it)->IsMaster() == isMaster)
        return (*it);
    }
  }

  ibv_device **list = ibv_get_device_list(NULL);

  if (!devName && list[0])
    devName = defaultDevname = ibv_get_device_name(list[0]);

  for (int i = 0; list[i]; ++i) {
    if (!strcmp(devName, ibv_get_device_name(list[i]))) {
      try {
        RdmaResource *ret = new RdmaResource(list[i], isMaster);
        resources.push_back(ret);
        return ret;
      } catch (int err) {
        epicLog(LOG_FATAL, "Unable to get rdam resource\n");
        return NULL;
      }
    }
  }
  return NULL;
}

/*
 * TODO: check whether it is necessary if we already use the epoll mechanism
 */
bool RdmaResource::GetCompEvent() const {
  struct ibv_cq *ev_cq;
  void *ev_ctx;
  int ret;
  ret = ibv_get_cq_event(channel, &ev_cq, &ev_ctx);
  if (ret) {
    epicLog(LOG_FATAL, "Failed to get cq_event\n");
    return false;
  }
  /* Ack the event */
  ibv_ack_cq_events(ev_cq, 1);

  /* Request notification upon the next completion event */
  ret = ibv_req_notify_cq(ev_cq, 0);
  if (ret) {
    fprintf(stderr, "Couldn't request CQ notification\n");
    return false;
  }
  return true;
}

RdmaContext* RdmaResource::NewRdmaContext(bool isForMaster) {
  rdma_context_counter++;

  int s = isForMaster ? MAX_MASTER_PENDING_MSG : MAX_WORKER_PENDING_MSG;
  s = s > HW_MAX_PENDING ? HW_MAX_PENDING : s;
  if (RegCommSlot(s)) {
    epicLog(LOG_WARNING, "unable to register more communication slots\n");
    return nullptr;
  }
  epicLog(LOG_DEBUG, "new RdmaContext: %d\n", rdma_context_counter);
  return new RdmaContext(this, isForMaster);
}

void RdmaResource::DeleteRdmaContext(RdmaContext* ctx) {
  rdma_context_counter--;
  //TODO: de-regsiter the slots
  epicLog(LOG_DEBUG, "delete RdmaContext: %d\n", rdma_context_counter);
  delete ctx;
}

RdmaContext::RdmaContext(RdmaResource *res, bool master)
    : resource(res),
      isForMaster(master),
      msg(),
      pending_msg(0),
      pending_send_msg(0) {
  //check either master == true, or both isMaster() in RdmaContext and RdmaResouce are false
  epicAssert(IsMaster() || IsMaster() == res->IsMaster());

  max_pending_msg =
      IsMaster() ? MAX_MASTER_PENDING_MSG : MAX_WORKER_PENDING_MSG;
  max_pending_msg =
      max_pending_msg > HW_MAX_PENDING ? HW_MAX_PENDING : max_pending_msg;
  int max_buf_size = IsMaster() ? MASTER_BUFFER_SIZE : WORKER_BUFFER_SIZE;

  void* buf = zmalloc(roundup(max_buf_size, page_size));
  if (unlikely(!buf)) {
    epicLog(LOG_WARNING, "Unable to allocate memeory\n");
    goto send_buf_err;
  }

  //init the send buf
  send_buf = ibv_reg_mr(res->pd, buf, max_buf_size,
                        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
  if (unlikely(!send_buf)) {
    epicLog(LOG_WARNING, "Unable to register mr\n");
    goto send_mr_err;
  }

  slot_head = slot_tail = 0;
  pending_msg = to_signaled_send_msg = to_signaled_w_r_msg = 0;
  max_unsignaled_msg =
      MAX_UNSIGNALED_MSG > max_pending_msg ?
          max_pending_msg : MAX_UNSIGNALED_MSG;
  //because we're using uint16_t to represent currently to_be_signalled msg
  epicAssert(max_unsignaled_msg <= USHRT_MAX);
  full = false;

  {
    ibv_qp_init_attr attr = { };
    attr.srq = res->srq;
    attr.send_cq = res->cq;
    attr.recv_cq = res->cq;
    attr.qp_type = IBV_QPT_RC;
    attr.cap.max_send_wr = max_pending_msg;
    attr.cap.max_send_sge = 1;
    attr.sq_sig_all = 0;
//		attr.cap.max_recv_wr = 1;
//		attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = MAX_RDMA_INLINE_SIZE;

    qp = ibv_create_qp(res->pd, &attr);
  }
  if (unlikely(!qp)) {
    epicLog(LOG_WARNING, "Unable to create QP (%d:%s)\n", errno,
            strerror(errno));
    goto clean_mr;
  }

  {
    // set qp to init status
    ibv_qp_attr qattr = { };
    qattr.qp_state = IBV_QPS_INIT;
    qattr.pkey_index = 0;
    qattr.port_num = res->ibport;
    qattr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
        | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ;

    if (ibv_modify_qp(
        qp, &qattr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
      epicLog(LOG_WARNING, "Unable to modify qp to init status\n");
      goto clean_qp;
    }
  }

  {
    ibv_qp_init_attr attr = { };
    ibv_qp_attr qattr = { };
    if (ibv_query_qp(qp, &qattr, IBV_QP_CAP, &attr)) {
      epicLog(LOG_WARNING, "Unable to query qp");
      goto clean_qp;
    }
    epicLog(LOG_INFO,
            "qattr.cap.max_inline_data = %u, attr.cap.max_inline_data = %u",
            attr.cap.max_inline_data, qattr.cap.max_inline_data);
    if (attr.cap.max_inline_data == 0) {
      epicLog(LOG_WARNING, "Do NOT support inline data");
      MAX_RDMA_INLINE_SIZE = 0;
    }
  }

  return;

  clean_qp: ibv_destroy_qp(qp);
  clean_mr: ibv_dereg_mr(send_buf);
  send_mr_err: zfree(buf);
  send_buf_err: throw RDMA_CONTEXT_EXCEPTION;
}

int RdmaContext::SetRemoteConnParam(const char *conn) {
  int ret;
  uint32_t rlid, rpsn, rqpn, rrkey;
  uint64_t rvaddr;

  if (IsMaster()) {
    /* conn should be of the format "lid:qpn:psn" */
    sscanf(conn, "%x:%x:%x", &rlid, &rqpn, &rpsn);
  } else {
    /* conn should be of the format "lid:qpn:psn:rkey:vaddr" */
    sscanf(conn, "%x:%x:%x:%x:%lx", &rlid, &rqpn, &rpsn, &rrkey, &rvaddr);
    this->rkey = rrkey;
    this->vaddr = rvaddr;
  }

  /* modify qp to RTR state */
  {
    ibv_qp_attr attr = { };  //zero init the POD value (DON'T FORGET!!!!)
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_2048;
    attr.dest_qp_num = rqpn;
    attr.rq_psn = rpsn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = rlid;
    attr.ah_attr.src_path_bits = 0;
    //attr.ah_attr.sl = 1;
    attr.ah_attr.port_num = resource->ibport;

    ret = ibv_modify_qp(
        this->qp,
        &attr,
        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN
            | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (unlikely(ret)) {
      epicLog(LOG_WARNING, "Unable to modify qp to RTR (%d:%s)\n", errno,
              strerror(errno));
      return 1;
    }
  }

  {
    ibv_qp_attr attr = { };
    /* modify qp to rts state */
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = this->resource->psn;
    attr.max_rd_atomic = 1;
    ret = ibv_modify_qp(
        this->qp,
        &attr,
        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY
            | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);

    if (unlikely(ret)) {
      epicLog(LOG_WARNING, "Unable to modify qp to RTS state\n");
      return 1;
    }
  }

  resource->PostRecv(max_pending_msg);
  return 0;
}

const char* RdmaContext::GetRdmaConnString() {
  if (!msg) {
    if (IsMaster())
      msg = (char *) zmalloc(MASTER_RDMA_CONN_STRLEN + 1);  //1 for \0
    else
      msg = (char *) zmalloc(WORKER_RDMA_CONN_STRLEN + 1);
  }

  if (unlikely(!msg)) {
    epicLog(LOG_WARNING, "Unable to allocate memory\n");
    goto out;
  }

  /*
   * we use RDMA send/recv to do communication
   * for communication among workers, we also allow direct access to the whole memory space so that we expose the base addr and rkey
   */
  if (IsMaster()) {
    sprintf(msg, "%04x:%08x:%08x", this->resource->portAttribute.lid,
            this->qp->qp_num, this->resource->psn);
  } else {
    sprintf(msg, "%04x:%08x:%08x:%08x:%016lx",
            this->resource->portAttribute.lid, this->qp->qp_num,
            this->resource->psn, this->resource->bmr->rkey,
            (uintptr_t) this->resource->base);
  }
  out:
  epicLog(LOG_DEBUG, "msg = %s\n", msg);
  return msg;
}

char* RdmaContext::GetFreeSlot_() {
  int avail = RMINUS(slot_tail, slot_head, max_pending_msg);  //slot_head <= slot_tail ? slot_tail-slot_head : slot_tail+max_pending_msg-slot_head;
  if (!avail && !full)
    avail = max_pending_msg;
  epicLog(LOG_DEBUG, "avail = %d, pending_msg = %d", avail, pending_msg.load());
  if (avail <= 0 || pending_msg >= max_pending_msg) {
    epicLog(LOG_INFO, "all the slots are busy\n");
    return nullptr;
  }

  char* s = (char*) send_buf->addr + slot_head * MAX_REQUEST_SIZE;
  if (++slot_head == max_pending_msg)
    slot_head = 0;
  if (slot_head == slot_tail)
    full = true;
  return s;
}

char* RdmaContext::GetFreeSlot() {
  lock();
  char* s = GetFreeSlot_();
#ifdef ASYNC_RDMA_SEND
  unlock();  //we delay the unlock to after-send
#endif
  return s;
}

bool RdmaContext::IsRegistered(const void* addr) {
  return ((uintptr_t) addr >= (uintptr_t) send_buf->addr)
      && ((uintptr_t) addr < (uintptr_t) send_buf->addr + send_buf->length);
}

ssize_t RdmaContext::Rdma(ibv_wr_opcode op, const void* src, size_t len,
                          unsigned int id, bool signaled, void* dest,
                          uint32_t imm, uint64_t oldval, uint64_t newval) {
  epicLog(LOG_DEBUG, "op = %d, src = %lx, len = %d, id = %d, signaled = %d, dest = %lx, imm = %u, oldval = %lu, newval = %lu\nsrc = %s",
      op, src, len, id, signaled, dest, imm, oldval, newval, src);

  int ret = len;
  struct ibv_sge sge_list = { };
  struct ibv_send_wr wr = { };

  if (pending_msg >= max_pending_msg) {
    //add the send request to the waiting queue
    epicLog(LOG_INFO, "Rdma device is busy; will try later");
    pending_requests.push(RdmaRequest { op, src, len, id, signaled, dest, imm, oldval, newval });
    epicAssert(
               pending_requests.back().op == op && pending_requests.back().src == src
            && pending_requests.back().len == len
            && pending_requests.back().id == id
            && pending_requests.back().signaled == signaled
            && pending_requests.back().dest == dest
            && pending_requests.back().imm == imm
            && pending_requests.back().oldval == oldval
            && pending_requests.back().newval == newval);

    return -1;
  }

  if (op == IBV_WR_SEND) {
    if (unlikely(!IsRegistered(src) && len > MAX_RDMA_INLINE_SIZE)) {
      if (len > MAX_REQUEST_SIZE) {
        epicLog(LOG_WARNING, "len = %d, MAX_REQUEST_SIZE = %d, src = %s\n", len, MAX_REQUEST_SIZE, src);
        epicAssert(false);
      }
      char* sbuf = GetFreeSlot_();
      epicAssert(sbuf);
      memcpy(sbuf, src, len);
      zfree((void*) src);
      sge_list.addr = (uintptr_t) sbuf;
      pending_send_msg++;
    } else {
      if (IsRegistered(src)) {
        pending_send_msg++;
        epicLog(LOG_DEBUG, "Registered mem");
      }
      sge_list.addr = (uintptr_t) src;
    }
    sge_list.lkey = send_buf->lkey;
  } else if (op == IBV_WR_RDMA_WRITE || op == IBV_WR_RDMA_WRITE_WITH_IMM) {
    sge_list.addr = (uintptr_t) src;
    sge_list.lkey = resource->bmr->lkey;
    wr.wr.rdma.remote_addr = (uintptr_t) dest;
    wr.wr.rdma.rkey = rkey;
    if (op == IBV_WR_RDMA_WRITE_WITH_IMM) {
      wr.imm_data = htonl(imm);
    }
  } else if (op == IBV_WR_RDMA_READ) {
    sge_list.addr = (uintptr_t) dest;
    sge_list.lkey = resource->bmr->lkey;
    wr.wr.rdma.remote_addr = (uintptr_t) src;
    wr.wr.rdma.rkey = rkey;
  } else {
    epicLog(LOG_WARNING, "unsupported RDMA OP");
    return -1;
  }

  sge_list.length = len;

  wr.opcode = op;
  wr.wr_id = -1;
  wr.sg_list = &sge_list;
  wr.num_sge = len == 0 ? 0 : 1;
  wr.next = nullptr;
  wr.send_flags = 0;
  if (len <= MAX_RDMA_INLINE_SIZE)
    wr.send_flags = IBV_SEND_INLINE;

  pending_msg++;
  uint16_t curr_to_signaled_send_msg = pending_send_msg - to_signaled_send_msg;
  uint16_t curr_to_signaled_w_r_msg = pending_msg - pending_send_msg - to_signaled_w_r_msg;

  if (unlikely(curr_to_signaled_send_msg + curr_to_signaled_w_r_msg == max_unsignaled_msg
          || signaled)) {  //we signal msg for every max_unsignaled_msg
    wr.send_flags |= IBV_SEND_SIGNALED;
    if (wr.opcode == IBV_WR_SEND) {
      epicLog(LOG_INFO, "signaled %s\n", (char* )sge_list.addr);
    } else {
      epicLog(LOG_INFO, "signaled, op = %d", wr.opcode);
    }

    to_signaled_send_msg += curr_to_signaled_send_msg;
    epicAssert(to_signaled_send_msg == pending_send_msg);
    to_signaled_w_r_msg += curr_to_signaled_w_r_msg;
    epicAssert(to_signaled_send_msg + to_signaled_w_r_msg == pending_msg);
    epicAssert(curr_to_signaled_send_msg + curr_to_signaled_w_r_msg <= max_unsignaled_msg);

    /*
     * higher to lower: send_msg(16), w_r_msg(16), workid(32)
     */
    /* 
     * FIXME: only such work requests have their wr_id set, but it seems
     * that the wr_id of each completed work request will be checked
     * against to see if there are any pending invalidate WRs.
     */
    wr.wr_id = (id & HALF_BITS)
        + ((uint64_t) (curr_to_signaled_send_msg & QUARTER_BITS) << 48)
        + ((uint64_t) (curr_to_signaled_w_r_msg & QUARTER_BITS) << 32);
  }

  struct ibv_send_wr *bad_wr;
  if (ibv_post_send(qp, &wr, &bad_wr)) {
    epicLog(LOG_WARNING, "ibv_post_send failed (%d:%s)\n", errno, strerror(errno));
    return -2;
  }
  if (op == IBV_WR_SEND && !IsRegistered((void *) sge_list.addr)) {
    epicAssert(wr.send_flags & IBV_SEND_INLINE);
    zfree((void*) sge_list.addr);
  }
  return ret;
}

ssize_t RdmaContext::Send(const void* ptr, size_t len, unsigned int id,
                          bool signaled) {
#ifdef ASYNC_RDMA_SEND
  lock();
#endif
  //lock(); //we already lock when getting the send buf
  ssize_t ret = Rdma(IBV_WR_SEND, ptr, len, id, signaled);
  unlock();
  return ret;
}

int RdmaContext::Recv() {
  return 0;
}

ssize_t RdmaContext::Write(raddr dest, raddr src, size_t len, unsigned int id,
                           bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_RDMA_WRITE, src, len, id, signaled, dest);
  unlock();
  return ret;
}

ssize_t RdmaContext::WriteWithImm(raddr dest, raddr src, size_t len,
                                  uint32_t imm, unsigned int id,
                                  bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_RDMA_WRITE_WITH_IMM, src, len, id, signaled, dest,
                     imm);
  unlock();
  return ret;
}

ssize_t RdmaContext::Read(raddr dest, raddr src, size_t len, unsigned int id,
                          bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_RDMA_READ, src, len, id, signaled, dest);
  unlock();
  return ret;
}

ssize_t RdmaContext::Cas(raddr src, uint64_t oldval, uint64_t newval,
                         unsigned int id, bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_ATOMIC_CMP_AND_SWP, src, sizeof(uint64_t), id,
                     signaled, nullptr, 0, oldval, newval);
  unlock();
  return ret;
}

void RdmaContext::ProcessPendingRequests(int n) {
  //process pending rdma requests
  int size = pending_requests.size();
  epicLog(LOG_INFO, "pending_requests %d", size);
  //we must iterate all the current pending requests in order to ensure the original order: not necessary (updated)
  int i = 0, j = -1;

#ifdef MERGE_RDMA_REQUESTS
  struct ibv_send_wr wrs[n];
  struct ibv_sge sls[n];
  struct ibv_send_wr* prev_wr = nullptr;
  int buf_pos = 0;
  for (i = 0; i < size; i++) {
    RdmaRequest& r = pending_requests.front();
    if (r.op == IBV_WR_SEND) {
      //1: "\0", 1: used for debug "\0"
      if (prev_wr && prev_wr->opcode == IBV_WR_SEND
          && buf_pos + r.len + 1 + 1 <= MAX_REQUEST_SIZE && !r.signaled) {
        epicLog(LOG_INFO, "merged!");
        *((char*) prev_wr->sg_list->addr + buf_pos) = '\0';  //sep by \0
        memcpy((void*) ((char*) prev_wr->sg_list->addr + buf_pos + 1), r.src, r.len);
        zfree((void*) r.src);
        buf_pos += (1 + r.len);
        prev_wr->sg_list[0].length = buf_pos;
      } else {
        j++;
        if (j == n) {
          break;
        }
        epicAssert(pending_msg < max_pending_msg);
        epicAssert(r.len <= MAX_REQUEST_SIZE);
        epicAssert(!IsRegistered(r.src));
        char* sbuf = GetFreeSlot_(); //guaranteed to get a free slot
        epicAssert(sbuf);
        memcpy(sbuf, r.src, r.len);
        zfree((void*) r.src);

        sls[j].addr = (uintptr_t) sbuf;
        sls[j].lkey = send_buf->lkey;
        sls[j].length = r.len;

        wrs[j].opcode = r.op;
        wrs[j].wr_id = -1;
        wrs[j].sg_list = &sls[j];
        wrs[j].num_sge = r.len == 0 ? 0 : 1;
        wrs[j].send_flags = 0;

        //update prev_wr
        prev_wr = &wrs[j];
        buf_pos = r.len;

        pending_send_msg++;
        pending_msg++;
      }
    } else if (r.op == IBV_WR_RDMA_WRITE
        || r.op == IBV_WR_RDMA_WRITE_WITH_IMM) {
      j++;
      if (j == n) {
        break;
      }
      epicAssert(pending_msg < max_pending_msg);

      sls[j].addr = (uintptr_t) r.src;
      sls[j].lkey = resource->bmr->lkey;
      sls[j].length = r.len;

      wrs[j].wr.rdma.remote_addr = (uintptr_t) r.dest;
      wrs[j].wr.rdma.rkey = rkey;
      if (r.op == IBV_WR_RDMA_WRITE_WITH_IMM) {
        wrs[j].imm_data = htonl(r.imm);
      }

      wrs[j].opcode = r.op;
      wrs[j].wr_id = -1;
      wrs[j].sg_list = &sls[j];
      wrs[j].num_sge = r.len == 0 ? 0 : 1;
      wrs[j].send_flags = 0;

      //update prev_wr
      prev_wr = &wrs[j];
      buf_pos = 0;

      pending_msg++;
    } else if (r.op == IBV_WR_RDMA_READ) {
      j++;
      if (j == n) {
        break;
      }
      epicAssert(pending_msg < max_pending_msg);

      sls[j].addr = (uintptr_t) r.dest;
      sls[j].lkey = resource->bmr->lkey;
      sls[j].length = r.len;

      wrs[j].wr.rdma.remote_addr = (uintptr_t) r.src;
      wrs[j].wr.rdma.rkey = rkey;

      wrs[j].opcode = r.op;
      wrs[j].wr_id = -1;
      wrs[j].sg_list = &sls[j];
      wrs[j].num_sge = r.len == 0 ? 0 : 1;
      wrs[j].send_flags = 0;

      //update prev_wr
      prev_wr = &wrs[j];
      buf_pos = 0;

      pending_msg++;
    } else {
      epicLog(LOG_WARNING, "unsupported RDMA OP");
    }

    uint16_t curr_to_signaled_send_msg = pending_send_msg
        - to_signaled_send_msg;
    uint16_t curr_to_signaled_w_r_msg = pending_msg - pending_send_msg
        - to_signaled_w_r_msg;
    if (unlikely(
        curr_to_signaled_send_msg + curr_to_signaled_w_r_msg
            == max_unsignaled_msg || r.signaled)) {
      wrs[j].send_flags |= IBV_SEND_SIGNALED;
      if (wrs[j].opcode == IBV_WR_SEND) {
        epicLog(LOG_INFO, "signaled %s\n", (char* )sls[j].addr);
      } else {
        epicLog(LOG_INFO, "signaled, op = %d", wrs[j].opcode);
      }
      to_signaled_send_msg += curr_to_signaled_send_msg;
      epicAssert(to_signaled_send_msg == pending_send_msg);
      to_signaled_w_r_msg += curr_to_signaled_w_r_msg;
      epicAssert(to_signaled_send_msg + to_signaled_w_r_msg == pending_msg);
      epicAssert(
          curr_to_signaled_send_msg + curr_to_signaled_w_r_msg
              <= max_unsignaled_msg);
      wrs[j].wr_id = (r.id & HALF_BITS)
          + ((uint64_t) (curr_to_signaled_send_msg & QUARTER_BITS) << 48)
          + ((uint64_t) (curr_to_signaled_w_r_msg & QUARTER_BITS) << 32);
    }
    pending_requests.pop();
  }

  if (size) {
    if (i == size) {
      epicLog(LOG_INFO, "merged all");
      j++;  //so that wrs[j-1] is the last valid request even if we consume all the pending requests
    } else {
      epicLog(LOG_INFO, "partially merged");
    }
    for (int k = 0; k < j - 1; k++) {
      wrs[k].next = &wrs[k + 1];
      if (wrs[k].sg_list->length <= MAX_RDMA_INLINE_SIZE)
        wrs[k].send_flags |= IBV_SEND_INLINE;
    }
    wrs[j - 1].next = nullptr;
    if (wrs[j - 1].sg_list->length <= MAX_RDMA_INLINE_SIZE)
      wrs[j - 1].send_flags |= IBV_SEND_INLINE;

    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(qp, &wrs[0], &bad_wr)) {
      epicLog(LOG_WARNING, "ibv_post_send failed (%d:%s)\n", errno,
              strerror(errno));
    }
  } else {
    epicAssert(j == -1);
  }
#else
  for(i = 0; i < n && i < size; i++) {
    RdmaRequest& r = pending_requests.front();
    int ret = Rdma(r);
    epicAssert(ret != -1);
    pending_requests.pop();
  }
#endif

//	for(;i < size; i++) {
//		RdmaRequest& r = pending_requests.front();
//		pending_requests.push(r);
//		pending_requests.pop();
//	}
}

unsigned int RdmaContext::SendComp(ibv_wc& wc) {
  unsigned int id = wc.wr_id & HALF_BITS;
  uint16_t curr_to_signaled_send_msg = wc.wr_id >> 48;
  uint16_t curr_to_signaled_w_r_msg = wc.wr_id >> 32 & QUARTER_BITS;

  epicLog(LOG_INFO, "id = %u, %u, %u", id, curr_to_signaled_send_msg,
          curr_to_signaled_w_r_msg);

  lock();
  slot_tail += curr_to_signaled_send_msg;
  if (slot_tail >= max_pending_msg)
    slot_tail -= max_pending_msg;

  to_signaled_send_msg -= curr_to_signaled_send_msg;
  to_signaled_w_r_msg -= curr_to_signaled_w_r_msg;
  pending_msg -= (curr_to_signaled_send_msg + curr_to_signaled_w_r_msg);
  pending_send_msg -= curr_to_signaled_send_msg;

  epicAssert(curr_to_signaled_send_msg + curr_to_signaled_w_r_msg <= max_unsignaled_msg);
  epicAssert(to_signaled_send_msg + to_signaled_w_r_msg <= pending_msg);

  if (full && curr_to_signaled_send_msg)
    full = false;

  ProcessPendingRequests(curr_to_signaled_send_msg + curr_to_signaled_w_r_msg);

  unlock();
  return id;
}

unsigned int RdmaContext::WriteComp(ibv_wc& wc) {
  return SendComp(wc);
}

char* RdmaContext::RecvComp(ibv_wc& wc) {
  //FIXME: thread-safe
  //what if others grab this slot before the current thread finish its job
  char* ret = resource->GetSlot(wc.wr_id);
  //resource->ClearSlot(wc.wr_id);
  resource->recv_posted -= 1;
  return ret;
}

RdmaContext::~RdmaContext() {
  ibv_destroy_qp(qp);
  ibv_dereg_mr(send_buf);
  zfree(send_buf->addr);
  zfree(msg);
}
