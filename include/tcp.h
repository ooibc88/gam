// Copyright (c) 2018 The GAM Authors 

#ifndef CALLBACK_H_ 
#define CALLBACK_H_ 

#include "ae.h"
#include "rdma.h"
#include "settings.h"

/**
 * callback for connection
 * For tcp case (a remote client), the established tcp connection is only used
 * to exchange rdma parameters, and will be immediately closed after
 * the exchangement.
 *
 * For Unix Socket case, the incoming client is local, and the established 
 * socket will be used for messaging with this client. The first 4 bytes of
 * each message is same as the immediate data associated with
 * RDMA_WRITE_WITH_IMM verbs.
 */

aeFileProc AcceptTcpClientHandle;
aeFileProc ProcessRdmaRequestHandle;

#endif

