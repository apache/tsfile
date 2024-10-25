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

#ifndef COMPRESS_GZIP_COMPRESSOR_H
#define COMPRESS_GZIP_COMPRESSOR_H

#include "compressor.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"
#include "common/logger/elog.h"
#include "common/allocator/byte_stream.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <zlib.h>

#define DEFLATE_BUFFER_SIZE 512
#define INFLATE_BUFFER_SIZE 512



namespace storage
{

class GzipCompressor
{
public:
  GzipCompressor();
  ~GzipCompressor();
  int reset();
  void destroy() { end_zstream(); }
  int compress(char *uncompressed_buf,
               uint32_t uncompressed_buf_len,
               char *&compressed_buf,
               uint32_t &compressed_buf_len);
  void after_compress(char *compressed_buf)
  {
    ::free(compressed_buf);
  }
  int compress_into_bytestream(char *uncompressed_buf,
                               uint32_t uncompressed_buf_len,
                               common::ByteStream &out);
private:
  int init_zstream();
  int end_zstream();
private:
  z_stream compress_stream_;
  char compressed_buf[DEFLATE_BUFFER_SIZE];
  bool zstream_valid_;
};

class GzipDeCompressor
{
public:
  GzipDeCompressor();
  ~GzipDeCompressor();
  int reset();
  void destroy() { end_zstream(); }
  int uncompress(char *compressed_buf,
                 uint32_t compressed_buf_len,
                 char *&uncompressed_buf,
                 uint32_t &uncompressed_buf_len);
  void after_uncompress(char *uncompressed_buf)
  {
    ::free(uncompressed_buf);
  }
  int decompress_into_bytestream(char *compressed_buf,
                                 uint32_t compressed_buf_len,
                                 common::ByteStream &out);
private:
  int init_zstream();
  int end_zstream();
private:
  z_stream decompress_stream_;
  char decompressed_buf[INFLATE_BUFFER_SIZE];
  bool zstream_valid_;
};

class GZIPCompressor : public Compressor
{
public:
  GZIPCompressor() : gzip_compressor_(),
                     gzip_decompressor_() {}
  int reset(bool for_compress) OVERRIDE
  {
    if (for_compress) {
      return gzip_compressor_.reset();
    } else {
      return gzip_decompressor_.reset();
    }
  }

  void destroy() OVERRIDE
  {
    gzip_compressor_.destroy();
    gzip_decompressor_.destroy();
  }

  int compress(char *uncompressed_buf,
               uint32_t uncompressed_buf_len,
               char *&compressed_buf,
               uint32_t &compressed_buf_len) OVERRIDE
  {
    return gzip_compressor_.compress(uncompressed_buf,
                                     uncompressed_buf_len,
                                     compressed_buf,
                                     compressed_buf_len);
  }
  void after_compress(char *compressed_buf) OVERRIDE
  {
    gzip_compressor_.after_compress(compressed_buf);
  }

  int uncompress(char *compressed_buf,
                 uint32_t compressed_buf_len,
                 char *&uncompressed_buf,
                 uint32_t &uncompressed_buf_len) OVERRIDE
  {
    return gzip_decompressor_.uncompress(compressed_buf,
                                         compressed_buf_len,
                                         uncompressed_buf,
                                         uncompressed_buf_len);
  }
  void after_uncompress(char *uncompressed_buf) OVERRIDE
  {
    gzip_decompressor_.after_uncompress(uncompressed_buf);
  }
private:
  GzipCompressor gzip_compressor_;
  GzipDeCompressor gzip_decompressor_;
};

} // end namespace storage
 // end

#endif // COMPRESS_GZIP_COMPRESSOR_H
