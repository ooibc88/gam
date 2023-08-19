
#include "client.h"

#include <photon/common/alog-stdstring.h>
#include <photon/common/alog.h>
#include <photon/common/iovector.h>
#include <photon/net/socket.h>

void McsClient::rpc_msi(photon::net::EndPoint ep, 
      uint32_t work_id,
      uint64_t gaddr,
      uint32_t n,
      uint32_t l,
      uint32_t k,
      std::complex<float> T) {
  Msi::Request req;
  req.work_id = work_id;
  req.gaddr = gaddr;
  req.n = n;
  req.l = l;
  req.k = k;
  req.T = T;
  Msi::Response resp;
  int ret = 0;
  auto stub = pool->get_stub(ep, false);
  if (!stub) return;
  DEFER(pool->put_stub(ep, ret < 0));
  ret = stub->call<Msi>(req, resp);
}

void McsClient::rpc_add(photon::net::EndPoint ep, 
      uint32_t work_id,
      uint64_t gaddr,
      uint32_t a) {
    Add::Request req;
    req.work_id = work_id;
    req.gaddr = gaddr;
    req.a = a;
  Add::Response resp;
  int ret = 0;
  auto stub = pool->get_stub(ep, false);
  if (!stub) return;
  DEFER(pool->put_stub(ep, ret < 0));
  ret = stub->call<Add>(req, resp);    
}