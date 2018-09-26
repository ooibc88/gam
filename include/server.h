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


class ServerFactory;
class Server;
class Client;

class Server{
  private:
    unordered_map<uint32_t, Client*> qpCliMap; /* rdma clients */
    unordered_map<int, Client*> widCliMap; //map from worker id to region
    RdmaResource* resource;
    aeEventLoop* el;
    int sockfd;
    const Conf* conf;

    friend class ServerFactory;
    friend class Master;
    friend class Worker;
    friend class Cache;

  public:
    Client* NewClient(bool isMaster, const char* rdmaConn = nullptr);
    Client* NewClient(const char*);
    Client* NewClient();

    virtual bool IsMaster() = 0;
    virtual int GetWorkerId() = 0;

    void RmClient(Client *);

    Client* FindClient(uint32_t qpn);
    void UpdateWidMap();
    Client* FindClientWid(int wid);

    void ProcessRdmaRequest();
    virtual int PostAcceptWorker(int, void*) {return 0;}
    virtual int PostConnectMaster(int, void*) {return 0;}
    virtual void ProcessRequest(Client* client, WorkRequest* wr) = 0;
    virtual void FarmProcessRemoteRequest(Client* client, const char* msg, uint32_t size) = 0;
    virtual void FarmResumeTxn(Client*) = 0;
    virtual void ProcessRequest(Client* client, unsigned int id) {};
    virtual void CompletionCheck(unsigned int id) {};

    inline const string& GetIP() {return conf->worker_ip;}
    inline int GetPort() {return conf->worker_port;}

    virtual ~Server() {aeDeleteEventLoop(el);};
};
#endif
