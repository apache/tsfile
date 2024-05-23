
#ifndef STORAGE_TSFILE_COMPRESS_COMPRESSOR_H
#define STORAGE_TSFILE_COMPRESS_COMPRESSOR_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

namespace timecho
{
namespace storage
{

class Compressor
{
public:
  Compressor() {}
  virtual ~Compressor() {}
  // @for_compress
  //  true  - for compressiom
  //  false - for uncompression
  virtual int reset(bool for_compress) = 0;
  virtual void destroy() = 0;
  virtual int compress(char *uncompressed_buf,
                       uint32_t uncompressed_buf_len,
                       char *&compressed_buf,
                       uint32_t &compressed_buf_len) = 0;
  virtual void after_compress(char *compressed_buf) = 0;
  virtual int uncompress(char *compressed_buf,
                         uint32_t compressed_buf_len,
                         char *&uncompressed_buf,
                         uint32_t &uncompressed_buf_len) = 0;                       
  virtual void after_uncompress(char *uncompressed_buf) = 0;

  // TODO other style API
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_COMPRESS_COMPRESSOR_H

