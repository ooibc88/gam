#ifndef __DATABASE_UTILS_CLUSTER_HELPER_H__
#define __DATABASE_UTILS_CLUSTER_HELPER_H__

#include <netdb.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <string>

class ClusterHelper {
public:
  static std::string GetLocalHostName() {
    char hostname[256];
    hostname[255] = '\0';
    gethostname(hostname, 255);
    return std::string(hostname, strlen(hostname));
  }

  static std::string GetIpByHostName(const std::string& hostname) {
    hostent* record = gethostbyname(hostname.c_str());
    in_addr* addr = (in_addr*) record->h_addr;
    std::string ret = inet_ntoa(*addr);
    return ret;
  }
};

#endif
