#pragma once

#include <photon/net/socket.h>
#include <photon/rpc/rpc.h>
#include <photon/photon.h>
#include <photon/thread/workerpool.h>
#include "protocol.h"

  struct McsServer {
    std::unique_ptr<photon::rpc::Skeleton> skeleton;
    std::unique_ptr<photon::net::ISocketServer> server;
    photon::WorkPool* pool;

    McsServer()
        : skeleton(photon::rpc::new_skeleton()),
          server(photon::net::new_tcp_socket_server()),
          pool(new photon::WorkPool(8, 0, 0)){
        skeleton->register_service<Msi,Add>(this);
    }
    int do_rpc_service(Msi::Request* req, Msi::Response* resp,
        IOVector* iov, IStream*);
    int do_rpc_service(Add::Request* req, Add::Response* resp,
        IOVector* iov, IStream*);
    int serve(photon::net::ISocketStream* stream) {
        return skeleton->serve(stream, false);
    }

    void term() {
        server.reset();
        skeleton.reset();
    }

    int run(int port);
  };