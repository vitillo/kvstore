#ifndef PARALLEL_KV_STORE
#define PARALLEL_KV_STORE

#include <cassert>
#include <pthread.h>
#include <sched.h>
#include <algorithm>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "Buffer.hpp"
#include "ConcurrentQueue.hpp"
#include "Config.hpp"
#include "KVStore.hpp"

class Task {
public:
  Task(std::shared_ptr<KVStore> store) : m_store(store) {}
  virtual void run() = 0;

protected:
  std::shared_ptr<KVStore> m_store;
};

class AddTask: public Task {
public:
  AddTask(std::shared_ptr<KVStore> store, const Buffer &key, const Buffer &value): Task(store), m_key(key), m_value(value) {}

  virtual void run() {
    m_store->add(m_key, m_value);
  }

private:
  OwnedBuffer m_key;
  OwnedBuffer m_value;
};

class GetTask: public Task{
public:
  GetTask(std::shared_ptr<KVStore> store, const Buffer &key, std::promise<std::shared_ptr<Buffer>> &&promise): Task(store), m_key(key), m_promise(std::move(promise)) {}

  virtual void run() {
    auto value = m_store->get(m_key);
    m_promise.set_value(value);
  }

private:
  OwnedBuffer m_key;
  std::promise<std::shared_ptr<Buffer>> m_promise;
};

class TerminateTask: public Task {
public:
  TerminateTask(std::shared_ptr<KVStore> store): Task(store) {}
  virtual void run() {}
};

class RemoveTask: public Task {
public:
  RemoveTask(std::shared_ptr<KVStore> store, const Buffer &key): Task(store), m_key(key) {}

  virtual void run() {
    m_store->remove(m_key);
  }

private:
  OwnedBuffer m_key;
};

class DestroyTask: public Task {
public:
  DestroyTask(std::shared_ptr<KVStore> store): Task(store) {}
  virtual void run() {
    m_store->destroy();
  }
};

class KVStorePartition {
public:
  KVStorePartition(const Config & config, int partition) {
    unsigned num_cpus = std::thread::hardware_concurrency();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(partition % num_cpus, &cpuset);

    auto partition_config = Config::create_partition(config, partition);
    m_store = std::make_shared<KVStore>(partition_config);
    m_thread = std::make_shared<std::thread>(&KVStorePartition::run, this);

    int rc = pthread_setaffinity_np(m_thread->native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
  }

  ~KVStorePartition() {
    m_queue.push(std::make_shared<TerminateTask>(m_store));
    m_thread->join();
  }

  void add(const Buffer &key, const Buffer &value) {
    auto task = std::make_shared<AddTask>(m_store, key, value);
    m_queue.push(task);
  }

  std::future<std::shared_ptr<Buffer>> get(const Buffer &key) {
    std::promise<std::shared_ptr<Buffer>> promise;
    auto fut = promise.get_future();
    auto task = std::make_shared<GetTask>(m_store, key, std::move(promise));
    m_queue.push(task);
    return fut;
  }

  void remove(const Buffer &key) {
    auto task = std::make_shared<RemoveTask>(m_store, key);
    m_queue.push(task);
  }

  void destroy() {
    auto task = std::make_shared<DestroyTask>(m_store);
    m_queue.push(task);
  }

private:
  void run() {
    while (true) {
      auto task = m_queue.pop();
      if (dynamic_cast<TerminateTask*>(task.get())) {
        break;
      } else {
        task->run();
      }
    }
  }

  std::shared_ptr<std::thread> m_thread;
  std::shared_ptr<KVStore> m_store;
  ConcurrentQueue<std::shared_ptr<Task>> m_queue;
};

class ParallelKVStore {
public:
  ParallelKVStore(const Config &config): m_config(config) {
    assert(config.parallelism > 0);

    for (auto i = 0; i < config.parallelism; i++) {
      m_stores.push_back(std::make_shared<KVStorePartition>(config, i));
    }
  }

  void add(const Buffer &key, const Buffer &value) {
    auto partition = get_partition(key);
    partition->add(key, value);
  }

  std::future<std::shared_ptr<Buffer>> get(const Buffer &key) {
    auto partition = get_partition(key);
    return partition->get(key);
  }

  void remove(const Buffer &key) {
    auto partition = get_partition(key);
    partition->remove(key);
  }

  void destroy() {
    for (auto &store : m_stores) {
      store->destroy();
    }
  }

private:
  std::shared_ptr<KVStorePartition> get_partition(const Buffer &key) {
    auto index = key.hash() % m_config.parallelism;
    return m_stores[index];
  }

  std::vector<std::shared_ptr<KVStorePartition>> m_stores;
  Config m_config;
};

#endif
