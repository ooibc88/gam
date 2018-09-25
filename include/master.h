// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_MASTER_H_
#define INCLUDE_MASTER_H_

#include <unordered_map>
#include <queue>
#include "ae.h"
#include "settings.h"
#include "log.h"
#include "server.h"

class Master : public Server {
  //the service thread
  thread* st;

  //worker list: ip:port,ip:port
  string worker_ips;

  //worker counter
  int workers;

  queue<Client*> unsynced_workers;
  unordered_map<uint64_t, pair<void*, Size>> kvs;

  unordered_map<uint64_t, queue<pair<Client*, WorkRequest*>>> to_serve_kv_request;

public:
  Master(const Conf& conf);
  inline void Join() {st->join();}

  inline bool IsMaster() {return true;}
  inline int GetWorkerId() {return 0;}

  void Broadcast(const char* buf, size_t len);

  void ProcessRequest(Client* client, WorkRequest* wr);
  //some post process after accepting a TCP connection (e.g., send the worker list)
  int PostAcceptWorker(int, void*);
  //inline int PostConnectMaster(int fd, void* data) {return 0;} //not used

  ~Master();
};

class MasterFactory {
  static Master *server;
 public:
  static Server* GetServer() {
    if (server)
      return server;
    else
      throw SERVER_NOT_EXIST_EXCEPTION;
  }
  static Master* CreateServer(const Conf& conf) {
    if (server)
      throw SERVER_ALREADY_EXIST_EXCEPTION;
    server = new Master(conf);
    return server;
  }
  ~MasterFactory() {
    if (server)
      delete server;
  }
};

#endif /* INCLUDE_MASTER_H_ */
