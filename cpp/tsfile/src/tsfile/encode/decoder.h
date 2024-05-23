
#ifndef STORAGE_TSFILE_ENCODE_DECODER_H
#define STORAGE_TSFILE_ENCODE_DECODER_H

#include "common/allocator/byte_stream.h"

namespace timecho
{
namespace storage
{

class Decoder
{
public:
  Decoder() {}
  virtual ~Decoder() {}
  virtual void reset() = 0;
  virtual bool has_remaining() = 0;
  virtual int read_boolean(bool &ret_value, common::ByteStream &in) = 0;
  virtual int read_int32(int32_t &ret_value, common::ByteStream &in) = 0;
  virtual int read_int64(int64_t &ret_value, common::ByteStream &in) = 0;
  virtual int read_float(float &ret_value, common::ByteStream &in) = 0;
  virtual int read_double(double &ret_value, common::ByteStream &in) = 0;
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_ENCODE_DECODER_H

