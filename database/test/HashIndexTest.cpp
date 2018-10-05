#include "HashIndex.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include "BenchmarkArguments.h"
#include "BenchmarkInitiator.h"
#include <boost/thread.hpp>

using namespace Database;

// thread sync
volatile bool is_begin = false;

static uint64_t GetKey(size_t partition_id, size_t thread_id, size_t op_id) {
  uint64_t ret = op_id;
  ret |= (partition_id << 55) | (thread_id << 45);
  return ret;
}
void ThreadExecute(size_t partition_id, size_t thread_id, HashIndex *hash_index) {
  while (!is_begin) ;
  GAlloc *gallocator = gallocators[thread_id];
  for (int i = 0; i < num_txn; ++i) {
    uint64_t key = GetKey(partition_id, thread_id, i);
    uint64_t value = key;
    GAddr data_addr = gallocator->Malloc(sizeof(uint64_t));
    gallocator->Write(data_addr, &value, sizeof(uint64_t));
    bool ret = hash_index->InsertRecord(key, data_addr, gallocator, thread_id);
    assert(ret);
  }
}
void ThreadCheck(size_t partition_id, size_t thread_id, HashIndex *hash_index) {
  while (!is_begin) ;
  GAlloc *gallocator = gallocators[thread_id];
  for (int i = 0; i < num_txn; ++i) {
    uint64_t key = GetKey(partition_id, thread_id, i);
    uint64_t exp_value = key;
    GAddr data_addr = hash_index->SearchRecord(
        key, gallocator, thread_id);
    uint64_t act_value= 0;
    gallocator->Read(data_addr, &act_value, sizeof(uint64_t));
    assert(act_value == exp_value);
  }
}
// example command: ./hash_index_test -p11111 -sf1 -sf1 -c4 -t1000000
int main(int argc, char *argv[]) {
  ArgumentsParser(argc, argv);
  std::string my_host_name = ClusterHelper::GetLocalHostName();
  ClusterConfig config(my_host_name, port, config_filename);
  ClusterSync synchronizer(&config);
   
  BenchmarkInitiator initiator(gThreadCount, &config);
  // gam storage
  initiator.InitGAllocator();
  synchronizer.Fence();
  // Initialize hash index
  GAddr storage_addr = Gnullptr;
  if (config.IsMaster()) {
    storage_addr = default_gallocator->AlignedMalloc(
        HashIndex::GetSerializeSize());
    HashIndex hash_index;
    hash_index.Init(kHashIndexBucketHeaderNum, default_gallocator);
    hash_index.Serialize(storage_addr, default_gallocator);
  }
  synchronizer.MasterBroadcast<GAddr>(&storage_addr);
  std::cout << "storage_addr=" << storage_addr << std::endl;
  HashIndex hash_index;
  hash_index.Deserialize(storage_addr, default_gallocator);
  synchronizer.Fence();
  std::cout << "start to insert to hash index" << std::endl;

  {
    boost::thread_group thread_group;
    for (size_t i = 0; i < gThreadCount; ++i) {
      thread_group.create_thread(
          boost::bind(&ThreadExecute, config.GetMyPartitionId(), i, &hash_index)
          );
    }
    is_begin = true;
    thread_group.join_all();
  }
  std::cout << "finish insert to hash index" << std::endl;
  synchronizer.Fence();

  // check
  is_begin = false;
  synchronizer.Fence();
  std::cout << "start check..." << std::endl;
  {
    boost::thread_group thread_group;
    for (size_t i = 0; i < gThreadCount; ++i) {
      thread_group.create_thread(
        boost::bind(&ThreadCheck, config.GetMyPartitionId(), i, &hash_index)
        );
    }
    is_begin = true;
    thread_group.join_all();
  } 
  std::cout << "finish check" << std::endl;
  synchronizer.Fence();
  
  std::cout << "exit..." << std::endl;
  return 0;
}
