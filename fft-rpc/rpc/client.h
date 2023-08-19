#pragma once

#include <photon/rpc/rpc.h>
#include <sys/uio.h>
#include "protocol.h"
struct McsClient {
    std::unique_ptr<photon::rpc::StubPool> pool;

    // create a tcp rpc connection pool
    // unused connections will be drop after 10 seconds(10UL*1000*1000)
    // TCP connection will failed in 1 second(1UL*1000*1000) if not accepted
    // and connection send/recv will take 5 socneds(5UL*1000*1000) as timedout
    McsClient()
        : pool(photon::rpc::new_stub_pool(10UL * 1000 * 1000, 1UL * 1000 * 1000,
                                          5UL * 1000 * 1000)) {}

void rpc_msi(photon::net::EndPoint ep, 
      uint32_t work_id,
      uint64_t gaddr,
      uint32_t n,
      uint32_t l,
      uint32_t k,
      std::complex<float> T);
      
void rpc_add(photon::net::EndPoint ep, 
      uint32_t work_id,
      uint64_t gaddr,
      uint32_t a);
};