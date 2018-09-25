// Copyright (c) 2018 The GAM Authors 


#include <sstream>
#include <cstring>
#include <cstdlib>
#include <stdio.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include "util.h"

template<>
vector<string>& Split<string>(stringstream& ss, vector<string>& elems,
                              char delim) {
  string item;
  while (getline(ss, item, delim)) {
    if (!item.empty())
      elems.push_back(item);
  }
  return elems;
}

struct timespec init_time;
void init() __attribute__ ((constructor));
void fini() __attribute__ ((destructor));
void init() {
  clock_gettime(CLOCK_REALTIME, &init_time);
}

void fini() {
}

/*
 * initial time that is used to avoid long overflow
 * return the current time in nanoseconds
 */
long get_time() {
//	struct timeval start;
//	gettimeofday(&start, NULL);
//	return start.tv_sec*1000l*1000+start.tv_usec;
  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);
  return (start.tv_sec - init_time.tv_sec) * 1000l * 1000 * 1000
      + (start.tv_nsec - init_time.tv_nsec);;
}

//get the ip address of the first interface
string get_local_ip(const char* iface) {
  int MAXINTERFACES = 16;
  char *ip = NULL;
  int fd, intrface;
  struct ifreq buf[MAXINTERFACES];
  struct ifconf ifc;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) >= 0) {
    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = (caddr_t) buf;
    if (!ioctl(fd, SIOCGIFCONF, (char *) &ifc)) {
      intrface = ifc.ifc_len / sizeof(struct ifreq);

      while (intrface-- > 0) {
        if (!(ioctl(fd, SIOCGIFADDR, (char *) &buf[intrface]))) {
          ip = (inet_ntoa(
              ((struct sockaddr_in*) (&buf[intrface].ifr_addr))->sin_addr));
          if (!iface || strcmp(buf[intrface].ifr_name, iface) == 0) {
            break;
          }
        }
      }
    }
    close(fd);
  }
  return string(ip);
}

unsigned char* mac_eth(char *eth) {
#define HWADDR_len 6
  int s, i;
  int mac_len = 17;
  unsigned char * MAC_str = (unsigned char*) malloc(sizeof(char) * 18);  //13
  struct ifreq ifr;
  s = socket(AF_INET, SOCK_DGRAM, 0);
  strcpy(ifr.ifr_name, eth);
  ioctl(s, SIOCGIFHWADDR, &ifr);
  for (i = 0; i < HWADDR_len - 1; i++) {
    sprintf((char*) &MAC_str[i * 3], "%02x:",
            ((char*) ifr.ifr_hwaddr.sa_data)[i]);
  }
  sprintf((char*) &MAC_str[i * 3], "%02x", ((char*) ifr.ifr_hwaddr.sa_data)[i]);
  MAC_str[mac_len] = '\0';
  return MAC_str;
}

char* get_hostname() {
  char *Name = (char*) malloc(150);
  memset(Name, 0, 150);
  gethostname(Name, 150);
  return Name;
}

char* get_ipbyname(char* name) {
  struct hostent *h;
  char * ip;

  /* get the host info */
  if ((h = gethostbyname(name)) == NULL) {
    herror("gethostbyname(): ");
    exit(1);
  } else {
    ip = inet_ntoa(*((struct in_addr *) h->h_addr));
  }
  return ip;
}

//  Windows
#ifdef _WIN32

#include <intrin.h>
uint64_t rdtsc() {
  return __rdtsc();
}

//  Linux/GCC
#else

uint64_t rdtsc() {
  unsigned int lo, hi;
  __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
  return ((uint64_t) hi << 32) | lo;
}

#endif

