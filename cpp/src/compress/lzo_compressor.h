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
#ifndef COMPRESS_LZO_COMPRESSOR_H
#define COMPRESS_LZO_COMPRESSOR_H

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/allocator/byte_stream.h"
#include "common/logger/elog.h"
#include "compressor.h"
#include "lzokay.hpp"
#include "utils/errno_define.h"
#include "utils/util_define.h"

#define UNCOMPRESSED_TIME 4

namespace storage {

class LZOCompressor : public Compressor {
   public:
    LZOCompressor() : compressed_buf_(nullptr), uncompressed_buf_(nullptr) {};
    ~LZOCompressor() {};
    // @for_compress
    //  true  - for compressiom
    //  false - for uncompression
    int reset(bool for_compress) OVERRIDE;
    void destroy() OVERRIDE;
    int compress(char *uncompressed_buf, uint32_t uncompressed_buf_len,
                 char *&compressed_buf, uint32_t &compressed_buf_len) OVERRIDE;
    void after_compress(char *compressed_buf) OVERRIDE;
    int uncompress(char *compressed_buf, uint32_t compressed_buf_len,
                   char *&uncompressed_buf,
                   uint32_t &uncompressed_buf_len) OVERRIDE;
    void after_uncompress(char *uncompressed_buf) OVERRIDE;

   private:
    char *compressed_buf_;
    char *uncompressed_buf_;
};

}  // end namespace storage
#endif  // COMPRESS_SNAPPY_COMPRESSOR_H
