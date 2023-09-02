#include "client_fft.h"
#include "client.h"
#include <photon/photon.h>
#include <photon/thread/workerpool.h>

static photon::net::EndPoint ep;
// photon::WorkPool pool(8,0,0);

void run_subfft(
    uint32_t work_id, 
    uint64_t addr_value, 
    unsigned int n, 
    unsigned int l, 
    unsigned int k, 
    std::complex<float> T,
    const char* ip_worker) {
  photon::init();
  McsClient client;
  ep = photon::net::EndPoint(photon::net::IPAddr(ip_worker),
                                98789);
  // pool.call([&] {
       printf("run subff: work_id %u addr_value %lu n %u\n l %u k %u T %lf\n",
        work_id,
        addr_value,
        n,
        l,
        k,
        T);
      client.rpc_msi(ep, work_id, addr_value, n, l, k, T);
    // });

}

void run_add(
    uint32_t work_id,
    uint32_t a,
    uint64_t addr_value,
    const char* ip_worker) {
  photon::init();
  McsClient client;
  ep = photon::net::EndPoint(photon::net::IPAddr(ip_worker),37730);
  client.rpc_add(ep, work_id, addr_value, a);
}