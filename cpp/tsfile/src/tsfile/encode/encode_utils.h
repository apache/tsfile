#ifndef STORAGE_TSFILE_ENCODE_ENCODE_UTILS_H
#define STORAGE_TSFILE_ENCODE_ENCODE_UTILS_H

#include "common/util_define.h"

namespace timecho
{
namespace storage
{

FORCE_INLINE int32_t number_of_leading_zeros(int32_t i)
{
  if (i == 0) {
    return 32;
  }
  int32_t n = 1;
  if (((uint32_t)i) >> 16 == 0) {
    n += 16;
    i <<= 16;
  }
  if (((uint32_t)i) >> 24 == 0) {
    n += 8;
    i <<= 8;
  }
  if (((uint32_t)i) >> 28 == 0) {
    n += 4;
    i <<= 4;
  }
  if (((uint32_t)i) >> 30 == 0) {
    n += 2;
    i <<= 2;
  }
  n -= ((uint32_t)i) >> 31;
  return n;
}

FORCE_INLINE int32_t number_of_trailing_zeros(int32_t i)
{
  if (i == 0) {
    return 32;
  }
  int32_t y;
  int32_t n = 31;
  y = i <<16;
  if (y != 0) {
    n = n -16;
    i = y;
  }
  y = i << 8;
  if (y != 0) {
    n = n - 8;
    i = y;
  }
  y = i << 4;
  if (y != 0) {
    n = n - 4;
    i = y;
  }
  y = i << 2;
  if (y != 0) {
    n = n - 2;
    i = y;
  }
  return n - (((uint32_t)(i << 1)) >> 31);
}

FORCE_INLINE int32_t number_of_leading_zeros(int64_t i)
{
  if (i == 0) {
    return 64;
  }
  int32_t n = 1;
  int32_t x = (int32_t)(((uint64_t)i) >> 32);
  if (x == 0) {
    n += 32;
    x = (int32_t)i;
  }
  if (((uint32_t)x) >> 16 == 0) {
    n += 16;
    x <<= 16;
  }
  if (((uint32_t)x) >> 24 == 0) {
    n += 8;
    x <<= 8;
  }
  if (((uint32_t)x) >> 28 == 0) {
    n += 4;
    x <<= 4;
  }
  if (((uint32_t)x) >> 30 == 0) {
    n += 2;
    x <<= 2;
  }
  n -= ((uint32_t)x) >> 31;
  return n;
}

FORCE_INLINE int32_t number_of_trailing_zeros(int64_t i)
{
  if (i == 0) {
    return 64;
  }
  int32_t x, y;
  int32_t n = 63;
  y = (int32_t)i; 
  if (y != 0) {
    n = n -32;
    x = y;
  } 
  else x = (int32_t)(((uint64_t)i) >> 32);
  y = x <<16; 
  if (y != 0) {
    n = n -16;
    x = y;
  }
  y = x << 8; 
  if (y != 0) {
    n = n - 8;
    x = y;
  }
  y = x << 4; 
  if (y != 0) {
    n = n - 4;
    x = y;
  }
  y = x << 2; 
  if (y != 0) {
    n = n - 2;
    x = y;
  }
  return n - (((uint32_t)(x << 1)) >> 31);
}

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_ENCODE_ENCODE_UTILS_H

