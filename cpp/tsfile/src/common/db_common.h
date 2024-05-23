
#ifndef COMMON_DB_COMMON_H
#define COMMON_DB_COMMON_H

#include <iostream>
#include "common/util_define.h"

namespace timecho
{
namespace common
{

enum TSDataType
{
  BOOLEAN = 0,
  INT32 = 1,
  INT64 = 2,
  FLOAT = 3,
  DOUBLE = 4,
  TEXT = 5,
  VECTOR = 6,
  NULL_TYPE = 254,
  INVALID_DATATYPE = 255
};

enum TSEncoding
{
  PLAIN = 0,
  DICTIONARY = 1,
  RLE = 2,
  DIFF = 3,
  TS_2DIFF = 4,
  BITMAP = 5,
  GORILLA_V1 = 6,
  REGULAR = 7,
  GORILLA = 8,
  ZIGZAG = 9,
  FREQ = 10,
  INVALID_ENCODING = 255
};

enum CompressionType
{
  UNCOMPRESSED = 0,
  SNAPPY = 1,
  GZIP = 2,
  LZO = 3,
  SDT = 4,
  PAA = 5,
  PLA = 6,
  LZ4 = 7,
  INVALID_COMPRESSION = 255
};

extern const char* s_data_type_names[7];
extern const char* s_encoding_names[12];
extern const char* s_compression_names[8];

FORCE_INLINE const char* get_data_type_name(TSDataType type)
{
  ASSERT(type >= BOOLEAN && type <= VECTOR);
  return s_data_type_names[type];
}

FORCE_INLINE const char* get_encoding_name(TSEncoding encoding)
{
  ASSERT(encoding >= PLAIN && encoding <= FREQ);
  return s_encoding_names[encoding];
}

FORCE_INLINE const char* get_compression_name(CompressionType type)
{
  return s_compression_names[type];
}

FORCE_INLINE TSEncoding get_default_encoding_for_type(TSDataType type)
{
  if (type == timecho::common::BOOLEAN) {
    return PLAIN;
  } else if (type == timecho::common::INT32) {
    return PLAIN;
  } else if (type == timecho::common::INT64) {
    return PLAIN;
  } else if (type == timecho::common::FLOAT) {
    return PLAIN;
  } else if (type == timecho::common::DOUBLE) {
    return PLAIN;
  } else if (type == timecho::common::TEXT) {
    return PLAIN;
  } else {
    ASSERT(false);
  }
  return INVALID_ENCODING;
}

FORCE_INLINE CompressionType get_default_compression_for_type(TSDataType type)
{
  if (type == timecho::common::BOOLEAN) {
    return UNCOMPRESSED;
  } else if (type == timecho::common::INT32) {
    return UNCOMPRESSED;
  } else if (type == timecho::common::INT64) {
    return UNCOMPRESSED;
  } else if (type == timecho::common::FLOAT) {
    return UNCOMPRESSED;
  } else if (type == timecho::common::DOUBLE) {
    return UNCOMPRESSED;
  } else if (type == timecho::common::TEXT) {
    return UNCOMPRESSED;
  } else {
    ASSERT(false);
  }
  return INVALID_COMPRESSION;
}

enum Ordering
{
  DESC,
  ASC
};

template<typename T>
FORCE_INLINE common::TSDataType GetDataTypeFromTemplateType() { return common::INVALID_DATATYPE; }

template<> FORCE_INLINE common::TSDataType GetDataTypeFromTemplateType<bool>() { return common::BOOLEAN; }
template<> FORCE_INLINE common::TSDataType GetDataTypeFromTemplateType<int32_t>() { return common::INT32; }
template<> FORCE_INLINE common::TSDataType GetDataTypeFromTemplateType<int64_t>() { return common::INT64; }
template<> FORCE_INLINE common::TSDataType GetDataTypeFromTemplateType<float>() { return common::FLOAT; }
template<> FORCE_INLINE common::TSDataType GetDataTypeFromTemplateType<double>() { return common::DOUBLE; }

FORCE_INLINE size_t get_data_type_size(TSDataType data_type)
{
  switch (data_type) {
    case timecho::common::BOOLEAN:
      return 1;
    case timecho::common::INT32:
    case timecho::common::FLOAT:
      return 4;
    case timecho::common::INT64:
    case timecho::common::DOUBLE:
      return 8;
    default:
      ASSERT(false);
      return 8;
  }
}

} // end namespace common

namespace storage
{

struct TimeRange
{
  TimeRange() : start_time_(INT64_MAX),
                end_time_(INT64_MIN) {}
  TimeRange(int64_t start_time, int64_t end_time) : start_time_(start_time), end_time_(end_time) {}

  int64_t start_time_;
  int64_t end_time_;
};

}

} // end namespace timecho

#endif

