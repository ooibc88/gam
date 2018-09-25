// Copyright (c) 2018 The GAM Authors 

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <stdint.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>
#include <assert.h>

#include "cuckoohash_map.hh"
#include "hashtable.h"

typedef uint32_t KeyType;
typedef std::string KeyType2;
typedef uint32_t ValType;

size_t power = 25;
size_t table_capacity = 0;
size_t thread_num = 1;
size_t begin_load = 0;
size_t end_load = 90;
size_t seed_ = 0;
size_t test_mode = 0;
size_t insert_percent = 10;
bool use_strings = false;

template<typename T>
T generateKey(size_t i) {
  return (T) i;
}

template<>
std::string generateKey<std::string>(size_t i) {
  const size_t min_length = 100;
  const std::string num(std::to_string(i));
  if (num.size() >= min_length)
    return num;
  std::string ret(min_length, 'a');
  const size_t startret = min_length - num.size();
  for (size_t i = 0; i < num.size(); ++i)
    ret[i + startret] = num[i];
  return ret;
}

template<typename Table>
class insert_thread {
 public:
  typedef typename std::vector<typename Table::key_type>::iterator it_t;
  static void func(Table& table, it_t begin, it_t end) {
    for (; begin != end; ++begin) {
      table.lock(*begin);
      assert(table.insert(*begin, 0));
      table.unlock(*begin);
    }
  }
};

template<typename Table>
class insert_thread_no_lock {
 public:
  typedef typename std::vector<typename Table::key_type>::iterator it_t;
  static void func(Table& table, it_t begin, it_t end) {
    for (; begin != end; ++begin)
      assert(table.insert(*begin, 0));
  }
};

template<typename Table>
class read_thread {
 public:
  typedef typename std::vector<typename Table::key_type>::iterator it_t;
  static void func(Table& table, it_t begin, it_t end,
                   std::atomic<size_t>& counter, bool in_table,
                   std::atomic<bool>& finished) {
    typename Table::mapped_type v;
    size_t reads = 0;
    while (!finished.load(std::memory_order_acquire)) {
      for (auto it = begin; it != end; ++it) {
        if (finished.load(std::memory_order_acquire)) {
          counter.fetch_add(reads);
          return;
        }
        table.lock(*it);
        assert(in_table == table.find(*it, v));
        //volatile bool ret = table.find(*it, v);
        table.unlock(*it);
        ++reads;
      }
    }
  }
};

template<typename Table>
class read_insert_thread {
 public:
  typedef typename std::vector<typename Table::key_type>::iterator it_t;
  static void func(Table& table, it_t begin, it_t end,
                   std::atomic<size_t>& counter, const double insert_prob,
                   const size_t start_seed) {
    typename Table::mapped_type v;
    std::mt19937_64 gen(start_seed);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    auto inserter_it = begin;
    auto reader_it = begin;
    size_t ops = 0;
    while (inserter_it != end) {
      if (dist(gen) < insert_prob) {
        table.lock(*inserter_it);
        assert(table.insert(*inserter_it, 0));
        table.unlock(*inserter_it);
        ++inserter_it;
      } else {
        table.lock(*reader_it);
        assert((reader_it < inserter_it) == table.find(*reader_it, v));
        table.unlock(*reader_it);
        ++reader_it;
        if (reader_it == end)
          reader_it = begin;
      }
      ++ops;
    }
    counter.fetch_add(ops);
  }
};

template<typename T>
class TestEnvironment {
  typedef typename T::key_type KType;
 public:
  TestEnvironment()
      : numkeys(1U << power),
        table("TEST_TABLE", table_capacity ? table_capacity : numkeys),
        keys(numkeys) {
    if (seed_ == 0) {
      seed_ = std::chrono::system_clock::now().time_since_epoch().count();
    }
    std::cout << "seed = " << seed_ << std::endl;
    gen.seed(seed_);
    keys[0] = numkeys;
    for (size_t i = 1; i < numkeys; ++i) {
      const size_t swapind = gen() % i;
      keys[i] = keys[swapind];
      keys[swapind] = generateKey<KType>(i + numkeys);
    }

    std::vector<std::thread> threads;
    size_t keys_per_thread = numkeys * (begin_load / 100.0) / thread_num;
    for (size_t i = 0; i < thread_num; ++i)
      threads.emplace_back(insert_thread_no_lock<T>::func, std::ref(table),
                           keys.begin() + i * keys_per_thread,
                           keys.begin() + (i + 1) * keys_per_thread);
    for (size_t i = 0; i < threads.size(); ++i)
      threads[i].join();
    std::cout << "Table with capacity " << numkeys
              << " prefilled to a load factor of " << begin_load << '%'
              << std::endl;
    init_size = table.size();
  }

  size_t numkeys;
  T table;
  std::vector<KType> keys;
  std::mt19937_64 gen;
  size_t init_size;
};

template<typename T>
void InsertThroughputTest(TestEnvironment<T>* env) {
  std::vector<std::thread> threads;
  size_t keys_per_thread = env->numkeys * ((end_load - begin_load) / 100.0)
      / thread_num;
  timeval t1, t2;
  gettimeofday(&t1, NULL);
  for (size_t i = 0; i < thread_num; ++i)
    threads.emplace_back(
        insert_thread<T>::func, std::ref(env->table),
        env->keys.begin() + i * keys_per_thread + env->init_size,
        env->keys.begin() + (i + 1) * keys_per_thread + env->init_size);
  for (size_t i = 0; i < thread_num; ++i)
    threads[i].join();
  gettimeofday(&t2, NULL);
  double elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000.0;
  elapsed_time += (t2.tv_usec - t1.tv_usec) / 1000.0;
  size_t num_inserts = env->table.size() - env->init_size;
  std::cout << "Insert throughput test: " << std::endl;
  std::cout << "----------Results----------" << std::endl;
  std::cout << "Final load factor:\t" << end_load << "%" << std::endl;
  std::cout << "Number of inserts:\t" << num_inserts << std::endl;
  std::cout << "Time elapsed:\t" << elapsed_time / 1000 << " seconds"
            << std::endl;
  std::cout << "Throughput: " << std::fixed
            << (double) num_inserts / (elapsed_time / 1000) << " inserts/sec"
            << std::endl;
}

template<typename T>
void ReadThroughputTest(TestEnvironment<T>* env) {
  std::vector<std::thread> threads;
  std::atomic<size_t> counter(0);
  std::atomic<bool> finished(false);

  const size_t first_readnum = thread_num * (begin_load / 100.0);
  const size_t second_readnum = thread_num - first_readnum;
  const size_t in_keys_per_thread =
      (first_readnum == 0) ? 0 : env->init_size / first_readnum;
  const size_t out_keys_per_thread = (env->numkeys - env->init_size)
      / second_readnum;
  for (size_t i = 0; i < first_readnum; ++i)
    threads.emplace_back(read_thread<T>::func, std::ref(env->table),
                         env->keys.begin() + i * in_keys_per_thread,
                         env->keys.begin() + (i + 1) * in_keys_per_thread,
                         std::ref(counter), true, std::ref(finished));
  for (size_t i = 0; i < second_readnum; ++i)
    threads.emplace_back(
        read_thread<T>::func, std::ref(env->table),
        env->keys.begin() + i * out_keys_per_thread + env->init_size,
        env->keys.begin() + (i + 1) * out_keys_per_thread + env->init_size,
        std::ref(counter), false, std::ref(finished));
  size_t test_len = 10;
  sleep(test_len);
  finished.store(true, std::memory_order_release);
  for (size_t i = 0; i < threads.size(); i++)
    threads[i].join();
  std::cout << "Read throughput test: " << std::endl;
  std::cout << "----------Results----------" << std::endl;
  std::cout << "Number of reads:\t" << counter.load() << std::endl;
  std::cout << "Time elapsed:\t" << test_len << " seconds" << std::endl;
  std::cout << "Throughput: " << std::fixed
            << counter.load() / (double) test_len << " reads/sec" << std::endl;
}

template<class T>
void ReadInsertThroughputTest(TestEnvironment<T>* env) {
  const size_t start_seed = (std::chrono::system_clock::now().time_since_epoch()
      .count());
  std::atomic<size_t> counter(0);
  std::vector<std::thread> threads;
  size_t keys_per_thread = env->numkeys * ((end_load - begin_load) / 100.0)
      / thread_num;
  timeval t1, t2;
  gettimeofday(&t1, NULL);
  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back(
        read_insert_thread<T>::func, std::ref(env->table),
        env->keys.begin() + (i * keys_per_thread) + env->init_size,
        env->keys.begin() + ((i + 1) * keys_per_thread) + env->init_size,
        std::ref(counter), (double) insert_percent / 100.0, start_seed + i);
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  gettimeofday(&t2, NULL);
  double elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000.0;  // sec to ms
  elapsed_time += (t2.tv_usec - t1.tv_usec) / 1000.0;      // us to ms
  std::cout << "Read & Insert throughput test: " << std::endl;
  std::cout << "----------Results----------" << std::endl;
  std::cout << "Final load factor:\t" << end_load << "%" << std::endl;
  std::cout << "Number of operations:\t" << counter.load() << std::endl;
  std::cout << "Time elapsed:\t" << elapsed_time / 1000 << " seconds"
            << std::endl;
  std::cout << "Throughput: " << std::fixed
            << (double) counter.load() / (elapsed_time / 1000) << " ops/sec"
            << std::endl;
}

void parse_flags(int argc, char** argv, const char* description,
                 const char* args[], size_t* arg_vars[], const char* arg_help[],
                 size_t arg_num, const char* flags[], bool* flag_vars[],
                 const char* flag_help[], size_t flag_num) {
  errno = 0;
  for (int i = 0; i < argc; i++) {
    for (size_t j = 0; j < arg_num; j++) {
      if (strcmp(argv[i], args[j]) == 0) {
        if (i == argc - 1) {
          std::cerr << "You must provide a positive integer argument"
                    << " after the " << args[j] << " argument" << std::endl;
          exit(EXIT_FAILURE);
        } else {
          size_t argval = strtoull(argv[i + 1], NULL, 10);
          if (errno != 0) {
            std::cerr << "The argument to " << args[j]
                      << " must be a valid size_t" << std::endl;
            exit(EXIT_FAILURE);
          } else {
            *(arg_vars[j]) = argval;
          }
        }
      }
    }
    for (size_t j = 0; j < flag_num; j++) {
      if (strcmp(argv[i], flags[j]) == 0) {
        *(flag_vars[j]) = true;
      }
    }
    if (strcmp(argv[i], "--help") == 0) {
      std::cerr << description << std::endl;
      std::cerr << "Arguments:" << std::endl;
      for (size_t j = 0; j < arg_num; j++) {
        std::cerr << args[j] << " (default " << *arg_vars[j] << "):\t"
                  << arg_help[j] << std::endl;
      }
      for (size_t j = 0; j < flag_num; j++) {
        std::cerr << flags[j] << " (default "
                  << (*flag_vars[j] ? "true" : "false") << "):\t"
                  << flag_help[j] << std::endl;
      }
      exit(EXIT_SUCCESS);
    }
  }
}

int main(int argc, char** argv) {
  const char* args[] = { "--power", "--table-capacity", "--thread-num",
      "--begin-load", "--end-load", "--seed", "--test-mode" };
  size_t* arg_vars[] = { &power, &table_capacity, &thread_num, &begin_load,
      &end_load, &seed_, &test_mode };
  const char* arg_help[] =
      { "The number of keys to size the table with, expressed as a power of 2",
          "The initial capacity of the table, expressed as a power of 2. "
              "If 0, the table is initialized to the number of keys",
          "The number of threads to spawn for each type of operation",
          "The load factor to fill the table up to before testing throughput",
          "The maximum load factor to fill the table up to when testing throughput",
          "The seed used by the random number generator",
          "Choice of test mode: 0 for read, 1 for insert, 2 for mix of read & insert" };
  const char* flags[] = { "--use-strings" };
  bool* flag_vars[] = { &use_strings };
  const char* flag_help[] = {
      "If set, the key type of the map will be std::string" };
  parse_flags(argc, argv, "A benchmark for inserts", args, arg_vars, arg_help,
              sizeof(args) / sizeof(const char*), flags, flag_vars, flag_help,
              sizeof(flags) / sizeof(const char*));

  if (begin_load >= 100) {
    std::cerr << "--begin-load must be between 0 and 99" << std::endl;
    exit(EXIT_FAILURE);
  } else if (begin_load >= end_load) {
    std::cerr << "--end-load must be greater than --begin-load" << std::endl;
    exit(EXIT_FAILURE);
  }

  if (use_strings) {
    auto* env = new TestEnvironment<HashTable<KeyType2, ValType>>;
    if (test_mode == 0)
      ReadThroughputTest(env);
    else if (test_mode == 1)
      InsertThroughputTest(env);
    else if (test_mode == 2)
      ReadInsertThroughputTest(env);
    delete env;
  } else {
    auto* env = new TestEnvironment<HashTable<KeyType, ValType>>;
    if (test_mode == 0)
      ReadThroughputTest(env);
    else if (test_mode == 1)
      InsertThroughputTest(env);
    else if (test_mode == 2)
      ReadInsertThroughputTest(env);
    delete env;
  }

  return EXIT_SUCCESS;
}
