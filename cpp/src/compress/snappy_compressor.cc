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
#include "snappy_compressor.h"

#include "common/allocator/alloc_base.h"

using namespace common;

namespace storage {

void SnappyCompressor::destroy() {
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }

    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

int SnappyCompressor::reset(bool for_compress) {
    // Nothing to do
    return E_OK;
}

int SnappyCompressor::compress(char *uncompressed_buf,
                               uint32_t uncompressed_buf_len,
                               char *&compressed_buf,
                               uint32_t &compressed_buf_len) {
    int ret = E_OK;
    size_t max_dst_size = snappy::MaxCompressedLength(uncompressed_buf_len);
    compressed_buf = (char *)mem_alloc(max_dst_size, MOD_COMPRESSOR_OBJ);
    if (compressed_buf == nullptr) {
        ret = E_OOM;
    } else {
        size_t compressed_len = 0;
        snappy::RawCompress(uncompressed_buf, uncompressed_buf_len,
                            compressed_buf, &compressed_len);
        if (compressed_buf == nullptr) {
            ret = E_COMPRESS_ERR;
        } else {
            char *compressed_data = (char *)mem_realloc(
                compressed_buf, static_cast<uint32_t>(compressed_len));
            if (compressed_data == nullptr) {
                ret = E_OOM;
            } else {
                compressed_buf = compressed_data;
                compressed_buf_ = compressed_data;
                compressed_buf_len = compressed_len;
            }
        }
    }

    return ret;
}

void SnappyCompressor::after_compress(char *compressed_buf) {
    if (compressed_buf != nullptr) {
        mem_free(compressed_buf);
    }
}

int SnappyCompressor::uncompress(char *compressed_buf,
                                 uint32_t compressed_buf_len,
                                 char *&uncompressed_buf,
                                 uint32_t &uncompressed_buf_len) {
    int ret = E_OK;
    char *regen_buffer = nullptr;

    size_t ulength;
    if (!snappy::GetUncompressedLength(compressed_buf, compressed_buf_len,
                                       &ulength) ||
        ulength > UINT32_MAX) {
        ret = E_COMPRESS_ERR;
    } else {
        regen_buffer = (char *)mem_alloc(static_cast<uint32_t>(ulength),
                                         MOD_COMPRESSOR_OBJ);
        if (regen_buffer == nullptr) {
            ret = E_OOM;
        } else {
            snappy::RawUncompress(compressed_buf, compressed_buf_len,
                                  regen_buffer);
            uncompressed_buf = regen_buffer;
            uncompressed_buf_ = regen_buffer;
            uncompressed_buf_len = static_cast<uint32_t>(ulength);
        }
    }
    return ret;
}

void SnappyCompressor::after_uncompress(char *uncompressed_buf) {
    if (uncompressed_buf != nullptr) {
        mem_free(uncompressed_buf);
    }
}

}  // end namespace storage