
#ifndef STORAGE_TSFILE_ENCODE_ENCODER_H
#define STORAGE_TSFILE_ENCODE_ENCODER_H

#include "common/allocator/byte_stream.h"

namespace timecho
{
namespace storage
{

class Encoder
{
public:
  Encoder() {}
  virtual ~Encoder() {}

  virtual void reset() = 0;
  virtual void destroy() = 0;
  // virtual int init(common::TSDataType data_type) = 0;
  virtual int encode(bool    value, common::ByteStream &out_stream) = 0;
  virtual int encode(int32_t value, common::ByteStream &out_stream) = 0;
  virtual int encode(int64_t value, common::ByteStream &out_stream) = 0;
  virtual int encode(float   value, common::ByteStream &out_stream) = 0;
  virtual int encode(double  value, common::ByteStream &out_stream) = 0;
  virtual int flush(common::ByteStream &out_stream) = 0;
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_ENCODE_ENCODER_H

