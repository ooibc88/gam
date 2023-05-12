// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_LOG_H_
#define INCLUDE_LOG_H_

#include <cassert>

#define LOG_TEST 0
#define LOG_PQ 1
#define LOG_FATAL 2
#define LOG_WARNING 3
#define LOG_INFO 4
#define LOG_DEBUG 5

#define MAX_LOGMSG_LEN    1024 /* Default maximum length of syslog messages */

#if defined __cplusplus && __GNUC_PREREQ (2,95)
# define __ASSERT_VOID_CAST static_cast<void>
#else
# define __ASSERT_VOID_CAST (void)
#endif

void _epicLog(char* file, char *func, int lineno, int level, const char *fmt, ...);
void PrintStackTrace();

//#ifdef NDEBUG
//#define epicLog(level, fmt, ...) (__ASSERT_VOID_CAST (0))
//#else
#define epicLog(level, fmt, ...) _epicLog ((char*)__FILE__, (char*)__func__, __LINE__, level, fmt, ## __VA_ARGS__)
//#endif

#ifdef NDEBUG
#define epicAssert(_e) (__ASSERT_VOID_CAST (0))
#else
#define epicAssert(_e) ((_e)?(void)0 : (epicLog(LOG_WARNING, #_e" Assert Failed"),PrintStackTrace(),assert(false)))
#endif
#define epicPanic(fmt, ...) epicLog(LOG_FATAL, fmt, ##__VA_ARGS__),exit(1)

#endif /* INCLUDE_LOG_H_ */
