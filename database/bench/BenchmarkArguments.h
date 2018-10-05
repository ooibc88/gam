// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_BENCHMARK_ARGUMENTS_H__
#define __DATABASE_BENCHMARK_ARGUMENTS_H__

#include <iostream>
#include <cassert>
#include "Meta.h"

namespace Database {
static int app_type = -1;
static double scale_factors[2] = { -1, -1 };
static int factor_count = 0;
static int dist_ratio = 1;
static int num_txn = -1;
static int num_core = -1;  // number of cores utilized in a single numa node.
static int port = -1;
static std::string config_filename = "config.txt";
// To modify tpcc workload
static size_t gReadRatio = 0;
static size_t gTimeLocality = 0;
static bool gForceRandomAccess = false; // fixed
static bool gStandard = true;  // true if follow standard specification


static void PrintUsage() {
  std::cout << "==========[USAGE]==========" << std::endl;
  std::cout << "\t-pINT: PORT(required)" << std::endl;
  std::cout << "\t-cINT: CORE_COUNT(required)" << std::endl;
  std::cout << "\t-sfINT: SCALE_FACTOR(required)" << std::endl;
  std::cout << "\t-sfDOUBLE: SCALE_FACTOR(required)" << std::endl;
  std::cout << "\t-tINT: TXN_COUNT(required)" << std::endl;
  std::cout << "\t-dINT: DIST_TXN_RATIO(optional,default=1)" << std::endl;
  //std::cout << "\t-zINT: BATCH_SIZE(optional)" << std::endl;
  std::cout << "\t-fSTRING: CONFIG_FILENAME(optional,default=config.txt)" << std::endl;
  std::cout << "\t-rINT: READ_RATIO(optional, [0,100])" << std::endl;
  std::cout << "\t-lINT: TIME_LOCALITY(optional, [0,100])" << std::endl;
  std::cout << "===========================" << std::endl;
  std::cout << "==========[EXAMPLES]==========" << std::endl;
  std::cout << "Benchmark -p11111 -c4 -sf10 -sf100 -t100000" << std::endl;
  std::cout << "==============================" << std::endl;
}

static void ArgumentsChecker() {
  if (port == -1) {
    std::cout << "PORT (-p) should be set" << std::endl;
    exit(0);
  }
  if (factor_count == 0) {
    std::cout << "SCALE_FACTOR (-sf) should be set." << std::endl;
    exit(0);
  }
  if (num_core == -1) {
    std::cout << "CORE_COUNT (-c) should be set." << std::endl;
    exit(0);
  }
  if (num_txn == -1) {
    std::cout << "TXN_COUNT (-t) should be set." << std::endl;
    exit(0);
  }
  if (!(dist_ratio >= 0 && dist_ratio <= 100)) {
    std::cout << "DIST_TXN_RATIO should be [0,100]." << std::endl;
    exit(0);
  }
  if (!(gReadRatio >= 0 && gReadRatio <= 100)) {
    std::cout << "READ_RATIO should be [0,100]." << std::endl;
    exit(0);
  }
  if (!(gTimeLocality >= 0 && gTimeLocality <= 100)) {
    std::cout << "TIME_LOCALITY should be [0,100]." << std::endl;
    exit(0);
  }
}

static void ArgumentsParser(int argc, char *argv[]) {
  if (argc <= 4) {
    PrintUsage();
    exit(0);
  }
  for (int i = 1; i < argc; ++i) {
    if (argv[i][0] != '-') {
      PrintUsage();
      exit(0);
    }
    if (argv[i][1] == 'p') {
      port = atoi(&argv[i][2]);
    } else if (argv[i][1] == 's' && argv[i][2] == 'f') {
      scale_factors[factor_count] = atof(&argv[i][3]);
      ++factor_count;
    } else if (argv[i][1] == 't') {
      num_txn = atoi(&argv[i][2]);
    } else if (argv[i][1] == 'd') {
      dist_ratio = atoi(&argv[i][2]);
    } else if (argv[i][1] == 'c') {
      num_core = atoi(&argv[i][2]);
      gThreadCount = num_core;
    } else if (argv[i][1] == 'f') {
      config_filename = std::string(&argv[i][2]);
    } else if (argv[i][1] == 'z') {
      gParamBatchSize = atoi(&argv[i][2]);
    } else if (argv[i][1] == 'r') {
      gReadRatio = atoi(&argv[i][2]);
      gStandard = false;
    } else if (argv[i][1] == 'l') {
      gTimeLocality = atoi(&argv[i][2]);
      gStandard = false;
    } else if (argv[i][1] == 'h') {
      PrintUsage();
      exit(0);
    } else {
      PrintUsage();
      exit(0);
    }
  }
  ArgumentsChecker();
}
}

#endif
