#ifndef TABLE_H
#define TABLE_H

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>

#include "AppendableMMap.hpp"
#include "Buffer.hpp"
#include "KeyValue.hpp"
#include "TableIterator.hpp"

class Table{
 public:
  typedef TableIterator const_iterator;

  std::shared_ptr<Buffer> get(const Buffer &key) {
    int64_t max = m_num_entries - 1;
    int64_t min = 0;

    while (min <= max) {
      auto half = (min + max) / 2;
      auto item = operator[](half);

      if (key < item.key) {
        max = half - 1;
      } else if (key > item.key){
        min = half + 1;
      } else {
        return std::make_shared<Buffer>(item.value);
      }
    }

    return nullptr;
  }

  KeyValue operator[](uint32_t i) {
    assert(i < m_num_entries);
    uint32_t offset = m_index[i];
    return KeyValue(m_mmap->data() + offset);
  }

  void delete_from_fs() {
    m_mmap->delete_from_fs();
  }

  const_iterator begin() {
    return TableIterator(m_mmap->data());
  }

  const_iterator end() {
    return TableIterator(reinterpret_cast<const char *>(m_end));
  }

  uint32_t size() const {
    return m_num_entries;
  }

  const char *data() {
    return m_mmap->data();
  }

  const Buffer min_key() const  {
    return m_min_key;
  }

  const Buffer max_key() const {
    return m_max_key;
  }

  Table(std::shared_ptr<AppendableMMap> mmap): m_mmap(mmap) {
    auto table_size = mmap->data() + mmap->size() - sizeof(uint32_t);
    m_num_entries = *reinterpret_cast<const uint32_t *>(table_size);
    m_index = reinterpret_cast<const uint32_t *>(table_size - sizeof(uint32_t)*m_num_entries);

    assert(m_num_entries != 0);

    auto last = operator[](m_num_entries - 1).value;
    m_end = last.data() + last.size();
    assert(m_end <= reinterpret_cast<const char *>(m_index));

    m_min_key = operator[](0).key;
    m_max_key = operator[](m_num_entries - 1).key;
  }

  static std::shared_ptr<Table> load_table(const std::string &path) {
    auto mmap = std::make_shared<AppendableMMap>(path);
    return std::make_shared<Table>(mmap);
  }

 private:
  const std::shared_ptr<AppendableMMap> m_mmap;
  const uint32_t *m_index;
  const char *m_end;
  uint32_t m_num_entries;
  Buffer m_min_key;
  Buffer m_max_key;
};

#endif
