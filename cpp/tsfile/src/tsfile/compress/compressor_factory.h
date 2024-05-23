

#ifndef STORAGE_TSFILE_COMPRESS_COMPRESSOR_FACTORY_H
#define STORAGE_TSFILE_COMPRESS_COMPRESSOR_FACTORY_H

#include "uncompressed_compressor.h"
#include "gzip_compressor.h"

namespace timecho
{
namespace storage
{

#define ALLOC_AND_RETURN_COMPRESSPR(CompressorClass)                                     \
  do {                                                                                   \
    void *buf = common::mem_alloc(sizeof(CompressorClass), common::MOD_COMPRESSOR_OBJ);  \
    if (buf != nullptr) {                                                                \
      CompressorClass *c = new (buf) CompressorClass;                                    \
      return c;                                                                          \
    } else {                                                                             \
      return nullptr;                                                                    \
    }                                                                                    \
  } while (false)

class CompressorFactory
{
public:
  static Compressor *alloc_compressor(common::CompressionType type)
  {
    if (type == common::UNCOMPRESSED) {
      ALLOC_AND_RETURN_COMPRESSPR(UncompressedCompressor);
    } else if (type == common::SNAPPY) {
      return nullptr;
    } else if (type == common::GZIP) {
      ALLOC_AND_RETURN_COMPRESSPR(GZIPCompressor);
    } else if (type == common::LZO) {
      return nullptr;
    } else if (type == common::SDT) {
      return nullptr;
    } else if (type == common::PAA) {
      return nullptr;
    } else if (type == common::PLA) {
      return nullptr;
    } else if (type == common::LZ4) {
      return nullptr;
    } else {
      ASSERT(false);
      return nullptr;
    }
  }

  static void free(Compressor *c)
  {
    common::mem_free(c);
  }
};

} // end namespace storage
} // end namespace timehco

#endif // STORAGE_TSFILE_COMPRESS_COMPRESSOR_FACTORY_H

