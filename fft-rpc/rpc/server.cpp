#include "server.h"


#include <fcntl.h>
#include <photon/common/alog-stdstring.h>
#include <photon/common/alog.h>
#include <sys/uio.h>
#include <unistd.h>
#include "fft.h"

int McsServer::do_rpc_service(Msi::Request* req, Msi::Response* resp,
  IOVector* iov, IStream*) {
    pool->call([&] {
      printf("run subff: work_id %u addr_value %lu n %u\n l %u k %u T %lf\n",
        req->work_id,
        req->gaddr,
        req->n,
        req->l,
        req->k,
        req->T);
      sub_fft(req->work_id, req->gaddr, req->n, req->l, req->k, req->T);
    });
    return 0;
}
int McsServer::do_rpc_service(Add::Request* req, Add::Response* resp,
        IOVector* iov, IStream*) {
    add(req->work_id, req->gaddr, req->a);
    return 0;
}

int McsServer::run(int port) {
  if (server->bind(port) < 0) {
      // LOG_ERRNO_RETURN(0, -1, "Failed to bind port `", port)
  }
  if (server->listen() < 0) {
    // LOG_ERRNO_RETURN(0, -1, "Failed to listen");
  }
  server->set_handler({this, &McsServer::serve});
  // LOG_INFO("Started rpc server at `", server->getsockname());
  return server->start_loop(true);
} 