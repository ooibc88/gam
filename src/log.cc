// Copyright (c) 2018 The GAM Authors 


#include <cstdarg>
#include <unistd.h>
#include <syscall.h>
#include <sys/time.h>
#include <execinfo.h>
#include "log.h"
#include "gallocator.h"

void _epicLogRaw(int level, const char* msg) {
  const char *c = ".-*#";
  FILE *fp;
  char buf[128];

  fp =
      (GAllocFactory::LogFile() == nullptr) ?
          (level <= LOG_FATAL ? stderr : stdout) :
          fopen(GAllocFactory::LogFile()->c_str(), "a");
  if (!fp)
    return;

  int off;
  struct timeval tv;

  gettimeofday(&tv, NULL);
  off = strftime(buf, sizeof(buf), "%d %b %H:%M:%S.", localtime(&tv.tv_sec));
  snprintf(buf + off, sizeof(buf) - off, "%03d", (int) tv.tv_usec / 1000);
  //fprintf(fp,"[%d] %s %c %s\n",(int)getpid(),buf,c[level],msg);
  fprintf(fp, "[%d] %s %c %s\n", (int) syscall(SYS_gettid), buf, c[level], msg);

  fflush(fp);

  if (GAllocFactory::LogFile())
    fclose(fp);
}

void _epicLog(char* file, char* func, int lineno, int level, const char *fmt,
              ...) {
  if (level > GAllocFactory::LogLevel())
    return;

  va_list ap;
  char msg[MAX_LOGMSG_LEN];

  int n = sprintf(msg, "[%s:%d-%s()] ", file, lineno, func);
  va_start(ap, fmt);
  vsnprintf(msg + n, MAX_LOGMSG_LEN - n, fmt, ap);
  va_end(ap);

  _epicLogRaw(level, msg);
}

void PrintStackTrace() {
  printf("\n***************Start Stack Trace******************\n");
  int size = 100;
  void *buffer[100];
  char **strings;
  int j, nptrs;
  nptrs = backtrace(buffer, size);
  printf("backtrace() returned %d addresses\n", nptrs);
  strings = backtrace_symbols(buffer, nptrs);
  if (strings == NULL) {
    perror("backtrace_symbols");
    exit(EXIT_FAILURE);
  }
  for (j = 0; j < nptrs; j++) {
    printf("%s\n", strings[j]);
  }
  free(strings);

  printf("\n***************End Stack Trace******************\n");
}

