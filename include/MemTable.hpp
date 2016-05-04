#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <cassert>
#include <cstdint>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include "Buffer.hpp"

class MemTable {
public:
  MemTable() {}

  // Used for testing purposes
  MemTable(const std::vector<std::tuple<std::string, std::string>> &table) {
    for (const auto &item : table) {
      m_table[std::get<0>(item)] = std::get<1>(item);
      m_size += std::get<0>(item).size() + std::get<1>(item).size();
    }
  }

  std::shared_ptr<Buffer> get(const Buffer &key) const {
    auto it = m_table.find(key);
    if (it != m_table.end()) {
      return std::shared_ptr<Buffer>(new OwnedBuffer(it->second));
    } else {
      return nullptr;
    }
  }

  void add(const Buffer &key, const Buffer &value) {
    auto ret = m_table.insert(std::pair<std::string, std::string>(key, value));
    if (ret.second == false) {
      m_size += -ret.first->second.size() + value.size();
      ret.first->second = value;
    } else {
      m_size += key.size() + value.size();
    }

    assert(m_size != 0);
  }

  void clear() {
    m_table.clear();
    m_size = 0;
  }

  uint32_t size() const {
    assert((m_size != 0 && m_table.size() != 0) || (m_size == 0 && m_table.size() == 0));
    return m_size;
  }

  const auto begin() const {
    return m_table.begin();
  }

  const auto end() const {
    return m_table.end();
  }

private:
  std::map<std::string, std::string> m_table;
  uint32_t m_size = 0;
};

#endif
