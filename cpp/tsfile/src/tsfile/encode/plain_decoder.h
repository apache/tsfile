
#ifndef STORAGE_TSFILE_ENCODE_PLAIN_DECODER_H
#define STORAGE_TSFILE_ENCODE_PLAIN_DECODER_H

namespace timecho
{
namespace storage
{

class PlainDecoder : public Decoder
{
public:
  FORCE_INLINE void reset() { /* do nothing */ }
  FORCE_INLINE bool has_remaining() { return false; }
  FORCE_INLINE int read_boolean(bool &ret_bool, common::ByteStream &in)
  {
    return common::SerializationUtil::read_ui8((uint8_t&)ret_bool, in);
  }

  FORCE_INLINE int read_int32(int32_t &ret_int32, common::ByteStream &in)
  {
    return common::SerializationUtil::read_var_int(ret_int32, in);
  }

  FORCE_INLINE int read_int64(int64_t &ret_int64, common::ByteStream &in)
  {
    return common::SerializationUtil::read_i64(ret_int64, in);
  }

  FORCE_INLINE int read_float(float &ret_float, common::ByteStream &in)
  {
    return common::SerializationUtil::read_float(ret_float, in);
  }

  FORCE_INLINE int read_double(double &ret_double, common::ByteStream &in)
  {
    return common::SerializationUtil::read_double(ret_double, in);
  }
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_ENCODE_PLAIN_DECODER_H

