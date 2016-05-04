#ifndef KVSTORE_H
#define KVSTORE_H

#include <cassert>
#include <memory>

#include "Buffer.hpp"
#include "Config.hpp"
#include "LSMTree.hpp"
#include "MemTable.hpp"

class KVStore{
public:
  KVStore(const Config &config): m_config(config) {
    m_tree = std::make_shared<LSMTree>(config);
  }

  ~KVStore() {
    if (!m_destroyed) {
      m_tree->dump_memtable(m_memtable);
    }
  }

  std::shared_ptr<Buffer> get(const Buffer &key) {
    assert(!m_destroyed);

    auto value = m_memtable.get(key);
    if (value == nullptr) {
      value = m_tree->get(key);
    }

    if (value == nullptr || *value == "") {
      return nullptr;
    }

    return value;
  }

  void add(const Buffer &key, const Buffer &value) {
    assert(!m_destroyed);
    assert(key.size() > 0 && value.size() > 0);

    m_memtable.add(key, value);

    if (m_memtable.size() > m_config.memtable_size) {
      m_tree->dump_memtable(m_memtable);
      m_memtable.clear();
    }
  }

  void remove(const Buffer &key) {
    assert(!m_destroyed);

    assert(key.size() > 0);
    m_memtable.add(key, "");
  }

  void destroy() {
    assert(!m_destroyed);

    m_memtable.clear();
    m_tree->destroy();
    m_destroyed = true;
  }

private:
  Config m_config;
  std::shared_ptr<LSMTree> m_tree;
  MemTable m_memtable;
  bool m_destroyed = false;
};

#endif
