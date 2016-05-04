#ifndef LEVEL_H
#define LEVEL_H

#include <cassert>
#include <cstdint>
#include <algorithm>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "Buffer.hpp"
#include "Config.hpp"
#include "FileSystem.hpp"
#include "MemTable.hpp"
#include "Table.hpp"
#include "TableBuilder.hpp"

class LevelN;

class Level {
public:
  Level(LevelConfig config): m_config(config) {
    if (config.overwrite) {
      delete_directory(config.path_level);
    }

    // Create directory if it doesn't exists; if it does load existing tables and sort them by their min key
    mkdir(config.path_db);
    mkdir(config.path_level);

    auto files = ls(config.path_level);
    for (auto &file : files) {
      m_tables.push_back(Table::load_table(path_append(config.path_level, file)));
    }

    // Level 0 tables are not contigous; as we load tables in sorted order
    // during construction we move Level 0 tables to Level 1 during destruction.
    if (config.level == 0) {
      assert(m_tables.size() == 0);
    }

    std::sort(m_tables.begin(), m_tables.end(), [](auto &x, auto &y){
      return x->min_key() < y->min_key();
    });
  }

  virtual std::shared_ptr<Buffer> get(const Buffer &) = 0;

  void destroy() {
    std::unique_lock<std::shared_timed_mutex> lock(m_mutex);
    m_tables.clear();
    delete_directory(m_config.path_level);
    delete_directory(m_config.path_db);
  }

  uint32_t size() {
    std::shared_lock<std::shared_timed_mutex> lock(m_mutex);
    return m_tables.size();
  }

  bool needs_merging() {
    std::shared_lock<std::shared_timed_mutex> lock(m_mutex);
    return m_tables.size() > m_config.threshold;
  }

  friend std::ostream& operator<< (std::ostream& stream, Level &level) {
    std::shared_lock<std::shared_timed_mutex> lock(level.m_mutex);
    stream << level.m_tables.size() << " tables";
    return stream;
  }

protected:
  LevelConfig m_config;
  std::vector<std::shared_ptr<Table>> m_tables;
  std::shared_timed_mutex m_mutex;
};

class Level0 : public Level {
public:
  Level0(LevelConfig config): Level(config) {}

  std::shared_ptr<Buffer> get(const Buffer &key) {
    std::shared_lock<std::shared_timed_mutex> lock(m_mutex);

    for (auto it = m_tables.rbegin(); it != m_tables.rend(); ++it) {
      auto table = *it;
      auto value = table->get(key);
      if (value != nullptr) {
        return value;
      }
    }

    return nullptr;
  }

  void dump_memtable(const MemTable &mem_table) {
    auto builder = TableBuilder(m_config.table_size, m_config.path_level);
    std::vector<std::shared_ptr<Table>> tables;


    for (const auto &item : mem_table) {
      if (!builder.add(item.first, item.second)) {
        tables.push_back(builder.finalize());
        auto res = builder.add(item.first, item.second);
        assert(res);
      }
    }

    auto last = builder.finalize();
    if (last) {
      tables.push_back(last);
    }

    std::unique_lock<std::shared_timed_mutex> lock(m_mutex);
    for (const auto &table : tables) {
      m_tables.push_back(table);
    }
  }

  friend LevelN;
};

class LevelN : public Level {
public:
  LevelN(LevelConfig config): Level(config) {}

  std::shared_ptr<Buffer> get(const Buffer &key) {
    std::shared_lock<std::shared_timed_mutex> lock(m_mutex);

    // Binary search on tables
    int32_t min = 0, max = m_tables.size() - 1;
    while (min <= max) {
      auto half = (min + max) / 2;
      auto &table = m_tables[half];

      if (key < table->min_key()) {
        max = half -1;
      } else if (key > table->max_key()) {
        min = half + 1;
      } else {
        return table->get(key);
      }
    }

    return nullptr;
  }

  void merge_with(std::shared_ptr<Level0> other) {
    std::unique_lock<std::shared_timed_mutex> level0_lock(other->m_mutex, std::defer_lock);
    decltype(level0_lock) level1_lock(m_mutex, std::defer_lock);

    // We need to lock as there are multiple writer threads for level 0.
    level0_lock.lock();
    decltype(m_tables) tmp;
    tmp.reserve(other->m_tables.size() + m_tables.size());
    tmp.insert(tmp.end(), other->m_tables.rbegin(), other->m_tables.rend());
    int level0_size = other->m_tables.size();
    level0_lock.unlock();

    // Find overlapping tables in current level
    auto first = m_tables.end();
    auto last = m_tables.end();
    auto min = tmp[0]->min_key();
    auto max = tmp[0]->max_key();

    for (const auto &table : tmp) {
      if (table->min_key() < min) {
        min = table->min_key();
      }
      if (table->max_key() > max) {
        max = table->max_key();
      }
    }

    for (auto i = m_tables.begin(); i < m_tables.end(); ++i) {
      auto &table = (*i);

      if (Buffer::min(table->max_key(), max) >= Buffer::max(table->min_key(), min)) {
        if (first == m_tables.end()) {
          first = i;
        }

        tmp.push_back(table);
        last = i;
      }
    }

    // Merge tables
    auto merged_tables = TableBuilder::merge_tables(tmp, m_config);

    // Update levels
    std::lock(level0_lock, level1_lock);
    other->m_tables.erase(other->m_tables.begin(), other->m_tables.begin() + level0_size);

    // Remove overlapping tables in current level and replace them with the merged ones
    if (last != m_tables.end()) {
      last = m_tables.erase(first, last + 1);
    }

    m_tables.insert(last, merged_tables.begin(), merged_tables.end());
  }

  void merge_with(std::shared_ptr<LevelN> other) {
    // No need to lock early here as there is only one writer thread for level 1 to N,
    // i.e. the list of tables can't change while we are reading them.
    decltype(m_tables) tmp;
    tmp.reserve(other->m_tables.size() + m_tables.size());
    tmp.insert(tmp.end(), other->m_tables.begin(), other->m_tables.end());

    // Find overlapping tables in current level
    auto min = other->m_tables[0]->min_key();
    auto max = other->m_tables.back()->max_key();
    auto first = m_tables.end();
    auto last = m_tables.end();

    for (auto i = m_tables.begin(); i < m_tables.end(); ++i) {
      auto &table = (*i);

      if (Buffer::min(table->max_key(), max) >= Buffer::max(table->min_key(), min)) {
        if (first == m_tables.end()) {
          first = i;
        }

        tmp.push_back(table);
        last = i;
      }
    }

    // Merge tables
    auto merged_tables = TableBuilder::merge_tables(tmp, m_config);

    // Update levels
    std::unique_lock<std::shared_timed_mutex> l1(other->m_mutex, std::defer_lock);
    decltype(l1) l2(m_mutex, std::defer_lock);
    std::lock(l1, l2);

    other->m_tables.clear();

    // Remove overlapping tables in current level and replace them with the merged ones
    if (last != m_tables.end()) {
      last = m_tables.erase(first, last + 1);
    }

    m_tables.insert(last, merged_tables.begin(), merged_tables.end());
  }
};

#endif
