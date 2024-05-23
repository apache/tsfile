#ifndef COMMON_CONTAINER_MURMUR3_MURMUR_HASH3_H
#define COMMON_CONTAINER_MURMUR3_MURMUR_HASH3_H

#include <string>
#include "common/db_utils.h"
#include "common/allocator/my_string.h"

namespace timecho 
{
namespace common
{

/*
 * Murmur128Hash has to follow the logic in Java IoTDB,
 * since the bitmap buffer will be serialized to tsfile.
 */
class Murmur128Hash
{
public:
  FORCE_INLINE static int32_t hash(int32_t value, uint32_t seed) 
  {
    return inner_hash(reinterpret_cast<const char *>(&value), 4, seed);
  }

  FORCE_INLINE static int32_t hash(int64_t value, uint32_t seed)
  {
    return inner_hash(reinterpret_cast<const char *>(&value), 8, seed);
  }

  FORCE_INLINE static int32_t hash(double value, uint32_t seed) 
  {
    return inner_hash(reinterpret_cast<const char *>(&value), 8, seed);
  }

  FORCE_INLINE static int32_t hash(std::string &value, uint32_t seed) 
  {
    return inner_hash(value.data(), static_cast<uint32_t >(value.size()), seed);
  }

  FORCE_INLINE static int32_t hash(const common::String &buf, int32_t seed)
  {
    return inner_hash(buf.buf_, buf.len_ - 1, seed);
  }
private:
  FORCE_INLINE static int64_t rotl64(int64_t v, int n)
  {
    uint64_t uv = (uint64_t)v;
    return ((uv << n) | (uv >> (64 - n)));
  }
  FORCE_INLINE static int64_t fmix(int64_t k)
  {
    uint64_t uk = (uint64_t)k;
    uk ^= uk >> 33;
    uk *= 0xff51afd7ed558ccdL;
    uk ^= uk >> 33;
    uk *= 0xc4ceb9fe1a85ec53L;
    uk ^= uk >> 33;
    return (int64_t)uk;
  }
  static int64_t get_block(const char *buf, int32_t index);
  static int64_t inner_hash(const char *buf, int32_t len, int64_t seed);
};

}  // end namespace common
}  // end namespace timecho

#endif // COMMON_CONTAINER_MURMUR3_MURMUR_HASH3_H
