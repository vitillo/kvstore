#ifndef LSMTREE_H
#define LSMTREE_H

#include <cassert>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "Buffer.hpp"
#include "Config.hpp"
#include "Level.hpp"
#include "MemTable.hpp"

class LSMTree {
public:
  LSMTree(const Config &config): m_config(config) {
    assert(m_config.levels.size() > 1);

    m_level0 = std::make_shared<Level0>(m_config.levels[0]);
    for (int i = 1; i < m_config.levels.size(); i++) {
      m_levels.push_back(std::make_shared<LevelN>(m_config.levels[i]));
    }

    m_merger = std::make_shared<std::thread>(&LSMTree::background_merger, this);
  }

  ~LSMTree() {
    if (m_terminate_merge) {
      return; // Tree was previously destroyed.
    }

    terminate_background_merger();
    // Level 0 tables are not contigous; as we load tables in sorted order
    // during construction we have to move Level 0 tables to Level 1.
    if (m_level0->size() > 0) {
      m_levels[0]->merge_with(m_level0);
    }
  }

  std::shared_ptr<Buffer> get(const Buffer &key) {
    assert(!m_terminate_merge);

    auto value = m_level0->get(key);
    if (value) {
      return value;
    }

    for (const auto &level : m_levels) {
      auto value = level->get(key);
      if (value) {
        return value;
      }
    }

    return nullptr;
  }

  void dump_memtable(const MemTable &mem_table) {
    assert(!m_terminate_merge);

    if (mem_table.size() == 0) {
      return;
    }

    m_level0->dump_memtable(mem_table);
    std::unique_lock<std::mutex> lock(m_mutex);
    m_new_data.notify_one();
  }

  void destroy() {
    if (m_terminate_merge) { // Return if tree has been already destroyed
      return;
    }

    terminate_background_merger();
    m_level0->destroy();
    for (auto &level : m_levels) {
      level->destroy();
    }
  }

  friend std::ostream& operator<< (std::ostream& stream, const LSMTree &tree) {
    stream << "level 0 - " << *tree.m_level0 << std::endl;
    for (int i = 0; i < tree.m_levels.size(); i++) {
      stream << "level " << i + 1 << " - " << *tree.m_levels[i] << std::endl;
    }
  }

private:
  void terminate_background_merger() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_terminate_merge = true;
    m_new_data.notify_one();
    lock.unlock();
    m_merger->join();
  }

  void background_merger() {
    std::unique_lock<std::mutex> lock(m_mutex);

    while (true) {
      m_new_data.wait(lock, [this](){
        return this->m_level0->needs_merging() || this->m_terminate_merge;
      });

      if (this->m_terminate_merge) {
        return;
      }

      if (m_level0->needs_merging()) {
        m_levels[0]->merge_with(m_level0);
      }

      for (int i = 1; i < m_levels.size(); i++) {
        auto &prev = m_levels[i - 1];
        auto &curr = m_levels[i];

        if (!prev->needs_merging()) {
          continue;
        }

        curr->merge_with(prev);
      }
    }
  }

  Config m_config;
  std::shared_ptr<Level0> m_level0;
  std::vector<std::shared_ptr<LevelN>> m_levels;

  std::shared_ptr<std::thread> m_merger;
  std::condition_variable m_new_data;
  std::mutex m_mutex;

  bool m_terminate_merge = false;
};

#endif
