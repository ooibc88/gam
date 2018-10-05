// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_UTILS_PROFILER_H__
#define __DATABASE_UTILS_PROFILER_H__

#include "TimeMeasurer.h"
#include <cstring>
#include <string>

namespace Database {
enum PhaseType
  : size_t {
    INDEX_READ,
  INDEX_INSERT,
  INDEX_INSERT_LOCK,
  INDEX_INSERT_MUTATE,
  INDEX_INSERT_GALLOCATE,
  CC_SELECT,
  CC_INSERT,
  CC_COMMIT,
  CC_ABORT,
  TXN_ABORT,
  TXN_EXECUTE,
  LOCK_READ_TEST,
  LOCK_WRITE_TEST,
  POPULATE_DISK,
  POPULATE_GALLOCATE,
  POPULATE_INSERT,
  kPhaseCount
};

const static std::string phase_type_string[kPhaseCount] = { "INDEX_READ",
    "INDEX_INSERT", "INDEX_INSERT_LOCK", "INDEX_INSERT_MUTATE",
    "INDEX_INSERT_GALLOCATE", "CC_SELECT", "CC_INSERT", "CC_COMMIT", "CC_ABORT",
    "TXN_ABORT", "TXN_EXECUTE", "LOCK_READ", "LOCK_WRITE", "POPULATE_DISK",
    "POPULATE_GALLOCATE", "POPULATE_INSERT" };

extern TimeMeasurer** profiler_timers;
extern long long** profile_elapsed_time;
}

#if defined(PROFILE)
#define INIT_PROFILE_TIME(thread_count) \
	profiler_timers = new TimeMeasurer*[thread_count]; \
	profile_elapsed_time = new long long*[thread_count]; \
	for (int i = 0; i < thread_count; ++i){ \
		profiler_timers[i] = new TimeMeasurer[kPhaseCount]; \
		profile_elapsed_time[i] = new long long[kPhaseCount]; \
		memset(profile_elapsed_time[i], 0, sizeof(long long) * kPhaseCount); \
	}

#define PROFILE_TIME_START(thread_id, phase_id) \
	profiler_timers[thread_id][phase_id].StartTimer();

#define PROFILE_TIME_END(thread_id, phase_id) \
	profiler_timers[thread_id][phase_id].EndTimer(); \
	profile_elapsed_time[thread_id][phase_id] += profiler_timers[thread_id][phase_id].GetElapsedNanoSeconds();

#define REPORT_PROFILE_TIME(thread_count)\
	std::cout << "************** profile time *****************" << std::endl; \
	for (int i = 0; i < thread_count; ++i){ \
		std::cout << "thread_id=" << i << ": "; \
	for (int j = 0; j < kPhaseCount; ++j){ \
		 std::cout << phase_type_string[j] << "=" << profile_elapsed_time[i][j] * 1.0 / 1000.0 / 1000.0 << "ms,";\
	} \
	std::cout << std::endl; \
	} \
	std::cout << "************** end profile time *****************" << std::endl;
#else
#define INIT_PROFILE_TIME(thread_count) ;
#define PROFILE_TIME_START(thread_id, phase_id) ;
#define PROFILE_TIME_END(thread_id, phase_id) ;
#define REPORT_PROFILE_TIME ;
#endif

#endif
