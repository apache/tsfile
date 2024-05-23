
#ifndef STORAGE_TSFILE_ENCODE_DECODER_FACTORY_H
#define STORAGE_TSFILE_ENCODE_DECODER_FACTORY_H

#include "decoder.h"
#include "plain_decoder.h"
#include "gorilla_decoder.h"
#include "ts2diff_decoder.h"

namespace timecho
{
namespace storage
{

#define ALLOC_AND_RETURN_DECODER(DecoderType)                                      \
  do {                                                                             \
    void *buf = common::mem_alloc(sizeof(DecoderType), common::MOD_DECODER_OBJ);   \
    DecoderType *decoder = nullptr;                                                \
    if (buf != nullptr) {                                                          \
      decoder = new (buf) DecoderType;                                             \
    }                                                                              \
    return decoder;                                                                \
  } while (false);

class DecoderFactory
{
public:
  static Decoder* alloc_time_decoder()
  {
    if (common::g_config_value_.time_encoding_type_ == common::PLAIN) {
      ALLOC_AND_RETURN_DECODER(PlainDecoder);
    } else if (common::g_config_value_.time_encoding_type_ == common::TS_2DIFF) {
      ALLOC_AND_RETURN_DECODER(LongTS2DIFFDecoder);
    } else {
      // not support now
      ASSERT(false);
      return nullptr;
    }
  }

  static Decoder* alloc_value_decoder(common::TSEncoding encoding,
                                      common::TSDataType data_type)
  {
    if (encoding == common::PLAIN) {
      ALLOC_AND_RETURN_DECODER(PlainDecoder);
    } else if (encoding == common::GORILLA) {
      if (data_type == common::INT32) {
        ALLOC_AND_RETURN_DECODER(IntGorillaDecoder);
      } else if (data_type == common::INT64) {
        ALLOC_AND_RETURN_DECODER(LongGorillaDecoder);
      } else if (data_type == common::FLOAT) {
        ALLOC_AND_RETURN_DECODER(FloatGorillaDecoder);
      } else if (data_type == common::DOUBLE) {
        ALLOC_AND_RETURN_DECODER(DoubleGorillaDecoder);
      } else {
        ASSERT(false);
        return nullptr;
      }
    } else if (encoding == common::TS_2DIFF) {
      if (data_type == common::INT32) {
        ALLOC_AND_RETURN_DECODER(IntTS2DIFFDecoder);
      } else if (data_type == common::INT64) {
        ALLOC_AND_RETURN_DECODER(LongTS2DIFFDecoder);
      } else if (data_type == common::FLOAT) {
        ALLOC_AND_RETURN_DECODER(FloatTS2DIFFDecoder);
      } else if (data_type == common::DOUBLE) {
        ALLOC_AND_RETURN_DECODER(DoubleTS2DIFFDecoder);
      } else {
        ASSERT(false);
      }
    } else {
      // not support now
      ASSERT(false);
      return nullptr;
    }
    return nullptr;
  }

  static void free(Decoder *decoder)
  {
    common::mem_free(decoder);
  }
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_ENCODE_DECODER_FACTORY_H

