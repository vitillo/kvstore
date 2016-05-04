#ifndef BUFFER_H
#define BUFFER_H

#include <iostream>
#include <cstdint>
#include <string>
#include <cassert>
#include <cstring>
#include <algorithm>

#include "AppendableMMap.hpp"

class Buffer{
 public:
  Buffer() {
    m_size = 0;
    m_buffer = nullptr;
  }

  // Warning: Reference only!
  Buffer(const std::string &x) {
    assert(x.size() < UINT16_MAX);
    m_size = x.size();
    m_buffer = x.c_str();
  }

  // Warning: Reference only!
  Buffer (const char *x) {
    auto len = strlen(x);
    assert(len < UINT16_MAX);
    m_size = len;
    m_buffer = x;
  }

  // Warning: Reference only
  Buffer (const void *x, uint32_t size) {
    assert(size < UINT16_MAX);
    m_size = size;
    m_buffer = reinterpret_cast<const char *>(x);
  }

  void serialize(AppendableMMap &mmap) const{
    mmap.appendFront((const char *) &m_size, sizeof(m_size));
    mmap.appendFront(m_buffer, m_size);
  }

  static Buffer deserialize(const char *raw) {
    auto buffer = Buffer();
    buffer.m_size = *(reinterpret_cast<const uint16_t *>(raw));
    buffer.m_buffer = reinterpret_cast<const char *>(raw + sizeof(uint16_t));
    return buffer;
  }

  const char * data() const{
    return m_buffer;
  }

  const uint16_t size() const{
    return m_size;
  }

  const uint32_t total_size() const {
    return m_size + sizeof(uint16_t);
  }

  int compare(const Buffer &that) const {
    auto size = std::min(m_size, that.m_size);
    auto cmp = memcmp(m_buffer, that.m_buffer, size);
    auto size_diff = m_size - that.m_size;

    if (cmp == 0) {
      return size_diff;
    } else {
      return cmp;
    }
  }

  // djb2 http://www.cse.yorku.ca/~oz/hash.html
  uint64_t hash() const {
    uint64_t hash = 5381;

    for (uint32_t i = 0; i < m_size; i++) {
      auto c = m_buffer[i];
      hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }

    return hash;
  }

  bool operator==(const Buffer &that) const {
    return compare(that) == 0;
  }

  bool operator!=(const Buffer &that) const {
    return compare(that) != 0;
  }

  bool operator<(const Buffer &that) const {
    return compare(that) < 0;
  }

  bool operator<=(const Buffer &that) const {
    return compare(that) <= 0;
  }

  bool operator>(const Buffer &that) const {
    return compare(that) > 0;
  }

  bool operator>=(const Buffer &that) const {
    return compare(that) >= 0;
  }

  operator std::string () const {
    return std::string(m_buffer, m_size);
  }

  static Buffer min(const Buffer &x, const Buffer &y) {
    return (x < y) ? x : y;
  }

  static Buffer max(const Buffer &x, const Buffer &y) {
    return (x > y) ? x : y;
  }

  friend std::ostream& operator<< (std::ostream& stream, const Buffer &buffer) {
    char tmp[buffer.m_size + 1];
    strncpy(tmp, buffer.m_buffer, buffer.m_size);
    tmp[buffer.m_size] = '\0';
    stream << "Length: " << buffer.m_size << ", Content: " << tmp;
    return stream;
  }

 protected:
  uint16_t m_size = 0;
  const char *m_buffer = nullptr;
};

class OwnedBuffer : public Buffer {
public:
  OwnedBuffer(const std::string &x): m_copy(x) {
    assert(m_copy.size() < UINT16_MAX);
    m_size = m_copy.size();
    m_buffer = m_copy.c_str();
  }

  OwnedBuffer(const Buffer &x): m_copy(x) {
    m_size = m_copy.size();
    m_buffer = m_copy.c_str();
  }

private:
  std::string m_copy;
};

#endif
