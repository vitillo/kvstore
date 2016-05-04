#ifndef TABLEBUILDER_H
#define TABLEBUILDER_H

#include <uuid/uuid.h>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "AppendableMMap.hpp"
#include "Buffer.hpp"
#include "Config.hpp"
#include "KeyValue.hpp"
#include "Table.hpp"
#include "TableIterator.hpp"

class TableBuilder{
public:
  typedef std::vector<std::shared_ptr<Table>> table_list;

  TableBuilder(uint32_t table_size = 1 << 20, const std::string &path=""): m_table_size(table_size),
                                                                           m_path(path) {
    clear();
  }

  bool add(const Buffer &key, const Buffer &value) {
    assert(key.size() != 0);

    initialize();

    if (current_size() + key.total_size() + value.total_size() + sizeof(uint32_t) > m_table_size) {
      return false;
    }

    m_index.push_back(m_mmap->head_index());
    key.serialize(*m_mmap);
    value.serialize(*m_mmap);
    return true;
  }

  uint32_t current_size() {
    return m_mmap->head_index() + sizeof(uint32_t)*m_index.size() + sizeof(uint32_t);
  }

  std::shared_ptr<Table> finalize() {
    if (m_mmap->head_index() == 0) {
      return nullptr;
    }

    uint32_t index_size = m_index.size();
    m_mmap->appendBack(&index_size, sizeof(uint32_t));
    m_mmap->appendBack(&m_index[0], sizeof(uint32_t)*index_size);
    auto res = std::make_shared<Table>(m_mmap);
    clear();
    return res;
  }

  static table_list merge_tables(const table_list &tables, LevelConfig config) {
    // Front tables have precedence over tail tables!
    TableBuilder builder(config.table_size, config.path_level);
    table_list result;
    Buffer last_added_key;

    std::vector<std::pair<std::shared_ptr<Table>, TableIterator>> iterators;
    for (const auto &it : tables) {
      it->delete_from_fs(); // Remove input tables from fs
      iterators.push_back(std::make_pair(it, it->begin()));
    }

    while (true) {
      if (iterators.empty()) {
        break;
      }

      auto min_index = 0;
      auto min_iterator = iterators[min_index].second;

      // Find min (assuming number of tables is small => no priority queue needed)
      for (int i = 1; i < iterators.size(); ++i) {
        auto &it = iterators[i].second;
        if (it->key.compare(min_iterator->key) < 0) {
          min_index = i;
          min_iterator = it;
        }
      }

      auto item = (*min_iterator);
      if (item.key != last_added_key) { // Ignore keys that have already been inserted
        if (!builder.add(item.key, item.value)) {
          result.push_back(builder.finalize());
          builder.add(item.key, item.value);
        }

        last_added_key = item.key;
      }

      // Remove empty input table
      if (++iterators[min_index].second == iterators[min_index].first->end()) {
        iterators.erase(iterators.begin() + min_index);
      }
    }

    auto last = builder.finalize();
    if (last != nullptr) {
      result.push_back(last);
    }

    return result;
  }

private:
  void clear() {
    m_mmap = nullptr;
    m_index.resize(0);
  }

  void initialize() {
    if (m_mmap == nullptr) {
      if (m_path.empty()) { // Anonymous mapping, used just for testing purposes
        m_mmap = std::make_shared<AppendableMMap>(m_table_size);
      } else {
        uuid_t uuid;
        char tmp[37];

        uuid_generate(uuid);
        uuid_unparse_lower(uuid, tmp);
        m_mmap = std::make_shared<AppendableMMap>(m_table_size, m_path + "/" + tmp);
      }
    }
  }

  std::shared_ptr<AppendableMMap> m_mmap;
  uint32_t m_table_size;
  std::vector<uint32_t> m_index;
  std::string m_path;
};


#endif
