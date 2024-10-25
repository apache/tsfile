/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "gzip_compressor.h"

using  namespace common;



namespace storage
{

GzipCompressor::GzipCompressor() : compressed_buf()
{
  zstream_valid_ = false;
}

GzipCompressor::~GzipCompressor()
{
  end_zstream();
}

int GzipCompressor::reset()
{
  int ret = E_OK;
  if (RET_FAIL(end_zstream())) {
  } else if (RET_FAIL(init_zstream())) {
  }
  return ret;
}

int GzipCompressor::init_zstream()
{
  if (zstream_valid_) {
    return E_OK;
  }
  compress_stream_.zalloc = (alloc_func)0;  // Z_NULL
  compress_stream_.zfree = (free_func)0;
  compress_stream_.opaque = (voidpf)0;
  compress_stream_.next_in = 0;
  compress_stream_.avail_in = 0;
  compress_stream_.next_out = 0;
  compress_stream_.avail_out = 0;

  memset(compressed_buf, 0, DEFLATE_BUFFER_SIZE);

  if (deflateInit2(&compress_stream_,
                   Z_DEFAULT_COMPRESSION,
                   Z_DEFLATED,
                   31,
                   8,
                   Z_DEFAULT_STRATEGY) != Z_OK) {
    //log_err("gzip deflateInit2 failed");
    return E_COMPRESS_ERR;
  }
  zstream_valid_ = true;
  return E_OK;
}

int GzipCompressor::end_zstream()
{
  if (!zstream_valid_) {
    return E_OK;
  }
  if(deflateEnd(&compress_stream_) != Z_OK) {
    //log_err("deflateEnd failed");
    return E_COMPRESS_ERR;
  }
  zstream_valid_ = false;
  return E_OK;
}

int GzipCompressor::compress_into_bytestream(char *uncompressed_buf,
                                             uint32_t uncompressed_buf_len,
                                             ByteStream &out)
{
  int ret = Z_OK;

  compress_stream_.next_in = (Bytef *)uncompressed_buf;
  compress_stream_.avail_in = uncompressed_buf_len;
  compress_stream_.next_out = (Bytef *)compressed_buf;
  compress_stream_.avail_out = DEFLATE_BUFFER_SIZE;

  if (uncompressed_buf == nullptr || uncompressed_buf_len == 0) {  // no more
    if (compress_stream_.next_out) {
      while (ret != Z_STREAM_END) {
        ret = deflate(&compress_stream_, Z_FINISH);
        if(ret != Z_OK && ret != Z_STREAM_END) {
          //log_err("deflate failed");
          return E_COMPRESS_ERR;
        }
        out.write_buf(compressed_buf, DEFLATE_BUFFER_SIZE -
        compress_stream_.avail_out); compress_stream_.next_out = (Bytef
        *)compressed_buf; compress_stream_.avail_out = DEFLATE_BUFFER_SIZE;
      }
    }
    return E_OK;
  }

  for (;;) {
    ret = deflate(&compress_stream_, Z_NO_FLUSH);
    if (ret != Z_OK) {
      //log_err("deflate failed");
      return E_COMPRESS_ERR;
    }

    if (compress_stream_.avail_in == 0) {  // current input data are all
      out.write_buf(compressed_buf, DEFLATE_BUFFER_SIZE -
      compress_stream_.avail_out); compress_stream_.next_out = (Bytef
      *)compressed_buf; compress_stream_.avail_out = DEFLATE_BUFFER_SIZE;
      break;
    }
    else if (compress_stream_.avail_out == 0) {  // no more space for output
      out.write_buf(compressed_buf, DEFLATE_BUFFER_SIZE);
      compress_stream_.next_out = (Bytef *)compressed_buf;
      compress_stream_.avail_out = DEFLATE_BUFFER_SIZE;
    }
  }

  return E_OK;
}

int GzipCompressor::compress(char *uncompressed_buf,
                             uint32_t uncompressed_buf_len,
                             char *&compressed_buf,
                             uint32_t &compressed_buf_len)
{
  int ret = E_OK;
  ByteStream out(DEFLATE_BUFFER_SIZE, MOD_COMPRESSOR_OBJ);
  if (RET_FAIL(compress_into_bytestream(uncompressed_buf,
  uncompressed_buf_len, out))) {
    return ret;
  }
  if (RET_FAIL(compress_into_bytestream(nullptr, 0, out))) {
    return ret;
  }
  compressed_buf = get_bytes_from_bytestream(out);
  compressed_buf_len = out.total_size();
  out.destroy();
  return ret;
}

GzipDeCompressor::GzipDeCompressor() : decompressed_buf()
{
  zstream_valid_ = false;
}

GzipDeCompressor::~GzipDeCompressor()
{
  end_zstream();
}

int GzipDeCompressor::init_zstream()
{
  if (zstream_valid_) {
    return E_OK;
  }
  decompress_stream_.zalloc = (alloc_func)0;  // Z_NULL
  decompress_stream_.zfree = (free_func)0;
  decompress_stream_.opaque = (voidpf)0;
  decompress_stream_.next_in = 0;
  decompress_stream_.avail_in = 0;
  decompress_stream_.next_out = 0;
  decompress_stream_.avail_out = 0;

  memset(decompressed_buf, 0, INFLATE_BUFFER_SIZE);

  if (inflateInit2(&decompress_stream_, 31) != Z_OK) {
    //log_err("inflateInit2 failed");
    return E_COMPRESS_ERR;
  }
  zstream_valid_ = true;
  return E_OK;
}

int GzipDeCompressor::end_zstream()
{
  if (!zstream_valid_) {
    return E_OK;
  }
  if(inflateEnd(&decompress_stream_) != Z_OK) {
    //log_err("inflateEnd failed");
    return E_COMPRESS_ERR;
  }
  zstream_valid_ = false;
  return E_OK;
}

int GzipDeCompressor::reset()
{
  int ret = E_OK;
  if (RET_FAIL(end_zstream())) {
  } else if (RET_FAIL(init_zstream())) {
  }
  return ret;
}

int GzipDeCompressor::decompress_into_bytestream(char *compressed_buf,
                                                 uint32_t compressed_buf_len,
                                                 ByteStream &out)
{
  int ret = Z_OK;

  decompress_stream_.next_in = (Bytef *)compressed_buf;
  decompress_stream_.avail_in = compressed_buf_len;
  decompress_stream_.next_out = (Bytef *)decompressed_buf;
  decompress_stream_.avail_out = INFLATE_BUFFER_SIZE;

  if (compressed_buf == nullptr || compressed_buf_len == 0) {
    if (decompress_stream_.next_out) {
      while (ret != Z_STREAM_END) {
        ret = inflate(&decompress_stream_, Z_FINISH);
        if(ret != Z_OK && ret != Z_STREAM_END) {
          //log_err("inflate failed");
          return E_COMPRESS_ERR;
        }
        out.write_buf(decompressed_buf, INFLATE_BUFFER_SIZE -
        decompress_stream_.avail_out); decompress_stream_.next_out = (Bytef
        *)decompressed_buf; decompress_stream_.avail_out =
        INFLATE_BUFFER_SIZE;
      }
    }
    return E_OK;
  }

  for (;;) {
    ret = inflate(&decompress_stream_, Z_NO_FLUSH);
    if (ret == Z_STREAM_END) {
      out.write_buf(decompressed_buf, INFLATE_BUFFER_SIZE -
      decompress_stream_.avail_out); break;
    }
    if (ret != Z_OK) {
      //log_err("inflate failed");
      return E_COMPRESS_ERR;
    }
    if (decompress_stream_.avail_in == 0) {
      out.write_buf(decompressed_buf, INFLATE_BUFFER_SIZE -
      decompress_stream_.avail_out); decompress_stream_.next_out = (Bytef
      *)decompressed_buf; decompress_stream_.avail_out = INFLATE_BUFFER_SIZE;
      break;
    }
    else if (decompress_stream_.avail_out == 0) {
      out.write_buf(decompressed_buf, INFLATE_BUFFER_SIZE);
      decompress_stream_.next_out = (Bytef *)decompressed_buf;
      decompress_stream_.avail_out = INFLATE_BUFFER_SIZE;
    }
  }

  return E_OK;
}

int GzipDeCompressor::uncompress(char *compressed_buf,
                                 uint32_t compressed_buf_len,
                                 char *&uncompressed_buf,
                                 uint32_t &uncompressed_buf_len)
{
  int ret = E_OK;
  ByteStream out(INFLATE_BUFFER_SIZE, MOD_COMPRESSOR_OBJ);
  if(RET_FAIL(decompress_into_bytestream(compressed_buf, compressed_buf_len,
  out))) {
    return ret;
  }
  if (RET_FAIL(decompress_into_bytestream(nullptr, 0, out))) {
    return ret;
  }
  uncompressed_buf = get_bytes_from_bytestream(out);
  uncompressed_buf_len = out.total_size();
//   uncompressed_buf[uncompressed_buf_len] = '\0';
  out.destroy();
  return ret;
}

} // end namespace storage
