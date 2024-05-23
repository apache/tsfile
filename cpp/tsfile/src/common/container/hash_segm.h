#ifndef COMMON_CONTAINER_HASH_SEGM_H
#define COMMON_CONTAINER_HASH_SEGM_H

#include <stddef.h>
#include "common/container/hash_node.h"
#include "common/errno_define.h"
#include "common/util_define.h" 
#include "common/mutex/mutex.h"

#define SEGMENT_CAPACITY 100

namespace timecho
{
namespace common
{

template <class KeyType, class ValueType>
class HashSegm {
public:
  HashSegm() {}
  ~HashSegm() {}

  FORCE_INLINE HashNode<KeyType, ValueType> & operator [] (size_t idx) 
  {
    if (idx >= SEGMENT_CAPACITY) {
      log_error("index %lu is out of range.", idx);
      ASSERT(idx < SEGMENT_CAPACITY);
    }      
    return segm_pointer_[idx];
  }

public:
  Mutex mutexes_[SEGMENT_CAPACITY];
  HashNode<KeyType, ValueType> segm_pointer_[SEGMENT_CAPACITY];
};


}  // end namespace common
}  // end namespace timecho

#endif  // COMMON_CONTAINER_HASH_SEGM_H