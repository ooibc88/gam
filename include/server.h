// Copyright (c) 2018 The GAM Authors 

#ifndef SERVER_H_
#define SERVER_H_ 

#include <unordered_map>

#include "client.h"
#include "settings.h"
#include "rdma.h"
#include "workrequest.h"
#include "structure.h"
#include "ae.h"
#include "hashtable.h"
#include "locked_unordered_map.h"
#include "map.h"

class ServerFactory;
class Server;
class Client;

class Server {
  private:
    //unordered_map<uint32_t, Client*> qpCliMap; /* rdma clients */
    //unordered_map<int, Client*> widCliMap; //map from worker id to region
    HashTable<uint32_t, Client*> qpCliMap { "qpCliMap" };  //thread-safe as it is dynamic
    HashTable<int, Client*> widCliMap { "widCliMap" };  //store all the wid -> Client map
    UnorderedMap<int, Client*> widCliMapWorker { "widCliMapWorker" };  //only store the wid -> Client map excluding ClientServer
    RdmaResource* resource;
    aeEventLoop* el;
    int sockfd;
    const Conf* conf;

    /* add xmx add */
    atomic<uint64_t> transferredBytes{0};
    atomic<uint64_t> racetime{0};
    atomic<uint64_t> requesttime{0};
    /* add xmx add */
#ifdef B_I
    atomic<uint64_t> read_miss{0};
    atomic<uint64_t> write_miss{0};
    atomic<uint64_t> write_hit{0};
    atomic<uint64_t> read_hit{0};
#endif

    friend class ServerFactory;
    friend class Master;
    friend class Worker;
    friend class Cache;

  public:

    /* add xmx add */
    uint64_t getTransferredBytes() const {
      return transferredBytes.load();
    }
    uint64_t getracetime() const{
      return racetime.load();
    }
    uint64_t getrequesttime() const {
      return requesttime.load();
    }

#ifdef B_I
    uint64_t getreadmiss() const {
      return read_miss.load();
    }
    uint64_t getreadhit() const{
      return read_hit.load();
    }
    uint64_t getwritemiss() const {
      return write_miss.load();
    }
    uint64_t getwritehit() const {
      return write_hit.load();
    }
#endif
    /* add xmx add */

    Client* NewClient(bool isMaster, const char* rdmaConn = nullptr);
    Client* NewClient(const char*);
    Client* NewClient();

    virtual bool IsMaster() = 0;
    virtual int GetWorkerId() = 0;

    void RmClient(Client *);

    Client* FindClient(uint32_t qpn);
    void UpdateWidMap(Client* cli);
    Client* FindClientWid(int wid);
    inline int GetClusterSize() {
      return widCliMap.size();
    }

    void ProcessRdmaRequest();
    void ProcessRdmaRequest(ibv_wc& wc);
    virtual int PostAcceptWorker(int, void*) {
      return 0;
    }
    virtual int PostConnectMaster(int, void*) {
      return 0;
    }
    virtual void ProcessRequest(Client* client, WorkRequest* wr) = 0;
    virtual void ProcessRequest(Client* client, unsigned int id) {}
    virtual void CompletionCheck(unsigned int id) {}

    const string& GetIP() const {
      return conf->worker_ip;
    }

    int GetPort() const {
      return conf->worker_port;
    }

    virtual ~Server() {
      aeDeleteEventLoop(el);
    }
};
#endif
