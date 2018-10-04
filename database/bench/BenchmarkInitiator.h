#ifndef __DATABASE_BENCHMARK_INITIATOR_H__
#define __DATABASE_BENCHMARK_INITIATOR_H__

#include "gallocator.h"
#include "ClusterHelper.h"
#include "ClusterConfig.h"
#include "StorageManager.h"
#include "Profiler.h"
#include "PerfStatistics.h"

namespace Database {
class BenchmarkInitiator {
 public:
  BenchmarkInitiator(const size_t& thread_count, 
      ClusterConfig* config)
      : thread_count_(thread_count),
        config_(config) {
  }

  void InitGAllocator() {
    ServerInfo master = config_->GetMasterHostInfo();
    ServerInfo myhost = config_->GetMyHostInfo();

    Conf* conf = new Conf();
    conf->loglevel = LOG_WARNING;
    conf->size = 1024 * 1024L * 512 * 2 * 16;
    conf->is_master = config_->IsMaster();
    conf->master_ip = ClusterHelper::GetIpByHostName(master.addr_);
    conf->worker_ip = ClusterHelper::GetIpByHostName(myhost.addr_);
    int partition_id = config_->GetMyPartitionId();
    // to avoid port conflicts on the same node
    conf->worker_port += partition_id;

    std::cout << "GAllocator config info: is_master=" << conf->is_master
              << ",master_ip=" << conf->master_ip << ",master_port="
              << conf->master_port << ",worker_ip=" << conf->worker_ip
              << ",worker_port=" << conf->worker_port << std::endl;

    default_gallocator = GAllocFactory::CreateAllocator(conf);
    std::cout << "create default gallocator" << std::endl;
    gallocators = new GAlloc*[thread_count_];
    for (size_t i = 0; i < thread_count_; ++i) {
      gallocators[i] = GAllocFactory::CreateAllocator(conf);
    }
  }

  GAddr InitStorage() {
    GAddr storage_addr = Gnullptr;
    int my_partition_id = config_->GetMyPartitionId();
    int partition_num = config_->GetPartitionNum();
    if (config_->IsMaster()) {
      // RecordSchema
      std::vector<RecordSchema*> schemas;
      this->RegisterSchemas(schemas);
      // StorageManager
      storage_addr = default_gallocator->AlignedMalloc(
          StorageManager::GetSerializeSize());
      this->RegisterTables(storage_addr, schemas);
    } 
    return storage_addr;
  }
  
 protected:
  virtual void RegisterTables(const GAddr& storage_addr, 
      const std::vector<RecordSchema*>& schemas) {}

  virtual void RegisterSchemas(std::vector<RecordSchema*>& schemas) {}

  const size_t thread_count_;
  ClusterConfig* config_;
};
}

#endif
