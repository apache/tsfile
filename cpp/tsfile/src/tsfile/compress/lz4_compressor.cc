#include "lz4_compressor.h"
#include "common/allocator/alloc_base.h"

using namespace timecho::common;

namespace timecho
{
namespace storage
{

void LZ4Compressor::destroy()
{
  if (compressed_buf_ != nullptr) {
    mem_free(compressed_buf_);
    compressed_buf_ = nullptr;
  }
  
  if (uncompressed_buf_ != nullptr) {
    mem_free(uncompressed_buf_);
    uncompressed_buf_ = nullptr;
  }
}

int LZ4Compressor::reset(bool for_compress)
{
  // Nothing to do
  return E_OK;
}

int LZ4Compressor::compress(char *uncompressed_buf,
                            uint32_t uncompressed_buf_len,
                            char *&compressed_buf,
                            uint32_t &compressed_buf_len)
{
  int ret = E_OK;
  int max_dst_size = LZ4_compressBound(uncompressed_buf_len);
  compressed_buf_ = (char*)mem_alloc((size_t)max_dst_size, MOD_COMPRESSOR_OBJ);

  if (compressed_buf_ != nullptr) {
    int compressed_data_size = LZ4_compress_default(uncompressed_buf, compressed_buf_, uncompressed_buf_len, max_dst_size);

    if (compressed_data_size <= 0) {
      ret = E_COMPRESS_ERR;
    } else {
      char* compressed_data = (char *)mem_realloc(compressed_buf_, (size_t)compressed_data_size);

      if (compressed_data == nullptr) {
        ret = E_COMPRESS_ERR;
      } else {
        compressed_buf_ = compressed_data;
        compressed_buf = compressed_data;
        compressed_buf_len = compressed_data_size;
      }
    }
  } else {
    ret = E_OOM;
  }
  return ret;
}

void LZ4Compressor::after_compress(char *compressed_buf)
{
  if (compressed_buf != nullptr) {
    mem_free(compressed_buf);
    compressed_buf = nullptr; // cppcheck-suppress uselessAssignmentPtrArg
  }
}

int LZ4Compressor::uncompress(char *compressed_buf,
                              uint32_t compressed_buf_len,
                              char *&uncompressed_buf,
                              uint32_t &uncompressed_buf_len)
{
  int ret = E_OK;
  float ratios[] = {1.5, 2.5, 3.5, 4.5, 255};
  for (int i = 0; i < UNCOMPRESSED_TIME; i++) {
    ret = uncompress(compressed_buf, 
                    compressed_buf_len,
                    uncompressed_buf,
                    uncompressed_buf_len, 
                    ratios[i]);
    if (ret == E_OK) {
      break;
    }
  }
  return ret;
}

int LZ4Compressor::uncompress(char *compressed_buf,
                              uint32_t compressed_buf_len,
                              char *&uncompressed_buf,
                              uint32_t &uncompressed_buf_len, float ratio)
{
  int ret = E_OK;
  char* regen_buffer = nullptr;
  int src_size = 0;
  if (ratio > 0) {
    src_size = (int)(compressed_buf_len * ratio);
    regen_buffer = (char*)mem_alloc(src_size, MOD_COMPRESSOR_OBJ);
  } else {
    ret = E_COMPRESS_ERR;
  }
  
  if (regen_buffer != nullptr) {
    int decompressed_size = LZ4_decompress_safe(compressed_buf, regen_buffer, compressed_buf_len, src_size);

    if (decompressed_size < 0) {
      // Release regen_buffer since decompress failed
      mem_free(regen_buffer);
      regen_buffer = nullptr;
      ret = E_COMPRESS_ERR;
    } else {
      uncompressed_buf = regen_buffer;
      uncompressed_buf_ = regen_buffer;
      uncompressed_buf_len = decompressed_size;
    }
  } else {
    ret = E_OOM;
  }
  return ret;
}

void LZ4Compressor::after_uncompress(char *uncompressed_buf)
{
  if (uncompressed_buf != nullptr) {
    mem_free(uncompressed_buf);
    uncompressed_buf = nullptr; // cppcheck-suppress uselessAssignmentPtrArg
  }
}

} // end namespace storage
} // end namespace timecho