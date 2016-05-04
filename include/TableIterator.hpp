#ifndef TABLEITERATOR_H
#define TABLEITERATOR_H

#include <iterator>

#include "Buffer.hpp"
#include "KeyValue.hpp"

class TableIterator : std::iterator<std::forward_iterator_tag, const KeyValue> {
public:
  KeyValue operator*() const {
    return KeyValue(m_current);
  }

  const KeyValue *operator->() {
    m_current_item = KeyValue(m_current);
    return &m_current_item;
  }

  bool operator==(const TableIterator &that) const {
    return m_current == that.m_current;
  }

  bool operator!=(const TableIterator &that) const {
    return m_current != that.m_current;
  }

  TableIterator& operator++() {
    auto kv = *(*this);
    m_current += kv.key.total_size() + kv.value.total_size();
    return *this;
  }

  TableIterator operator++(int) {
    auto tmp(*this);
    operator++();
    return tmp;
  }

private:
  friend class Table;

  TableIterator(const char *buffer) {
    m_current = buffer;
  }

  KeyValue m_current_item;
  const char *m_current = 0;
};

#endif
