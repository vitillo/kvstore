#ifndef KEYVALUE_H
#define KEYVALUE_H

#include "Buffer.hpp"

struct KeyValue{
public:
  KeyValue(const char *buffer): key(Buffer::deserialize(buffer)),
                                value(Buffer::deserialize(buffer + key.total_size())) {
  }

  KeyValue() {}

  Buffer key;
  Buffer value;
};

#endif
