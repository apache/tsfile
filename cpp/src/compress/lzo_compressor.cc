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
#include "lzo_compressor.h"

#include "common/allocator/alloc_base.h"

using namespace common;

namespace storage {

void LZOCompressor::destroy() {
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }

    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

int LZOCompressor::reset(bool for_compress) {
    // Nothing to do
    return E_OK;
}

int LZOCompressor::compress(char *uncompressed_buf,
                            uint32_t uncompressed_buf_len,
                            char *&compressed_buf,
                            uint32_t &compressed_buf_len) {
    int ret = E_OK;
    size_t max_dst_size = lzokay::compress_worst_size(uncompressed_buf_len);
    compressed_buf = (char *)mem_alloc(max_dst_size, MOD_COMPRESSOR_OBJ);
    if (compressed_buf == nullptr) {
        ret = E_OOM;
    } else {
        size_t compressed_len = 0;
        uint8_t *srcUint8 = reinterpret_cast<uint8_t *>(uncompressed_buf);
        uint8_t *dstUint8 = reinterpret_cast<uint8_t *>(compressed_buf);
        lzokay::EResult compress_result =
            lzokay::compress(srcUint8, uncompressed_buf_len, dstUint8,
                             max_dst_size, compressed_len);
        if (compress_result == lzokay::EResult::Success) {
            char *compress_data = (char *)mem_realloc(dstUint8, compressed_len);
            if (compress_data == nullptr) {
                ret = E_OOM;
            } else {
                compressed_buf = compress_data;
                compressed_buf_ = compress_data;
                compressed_buf_len = compressed_len;
            }
        } else {
            ret = E_COMPRESS_ERR;
        }
    }
    return ret;
}

void LZOCompressor::after_compress(char *compressed_buf) {
    if (compressed_buf != nullptr) {
        mem_free(compressed_buf);
    }
}

int LZOCompressor::uncompress(char *compressed_buf, uint32_t compressed_buf_len,
                              char *&uncompressed_buf,
                              uint32_t &uncompressed_buf_len) {
    int ret = E_OK;
    char *regen_buffer = nullptr;
    size_t ulength;
    constexpr float ratio[] = {1.5, 2.5, 3.5, 4.5, 255};
    for (uint8_t i = 0; i < UNCOMPRESSED_TIME; ++i) {
        regen_buffer = (char *)mem_alloc(compressed_buf_len * ratio[i],
                                         MOD_COMPRESSOR_OBJ);
        if (regen_buffer == nullptr) {
            ret = E_OOM;
        } else {
            lzokay::EResult result = lzokay::decompress(
                reinterpret_cast<uint8_t *>(compressed_buf), compressed_buf_len,
                reinterpret_cast<uint8_t *>(regen_buffer),
                compressed_buf_len * ratio[i], ulength);
            if (result != lzokay::EResult::Success) {
                mem_free(regen_buffer);
                regen_buffer = nullptr;
                ret = E_COMPRESS_ERR;
            } else {
                char *compress_data =
                    (char *)mem_realloc(regen_buffer, ulength);
                if (regen_buffer == nullptr) {
                    ret = E_OOM;
                } else {
                    ret = E_OK;
                    uncompressed_buf_len = ulength;
                    uncompressed_buf_ = compress_data;
                    uncompressed_buf = compress_data;
                    break;
                }
            }
        }
    }
    return ret;
}

void LZOCompressor::after_uncompress(char *uncompressed_buf) {
    if (uncompressed_buf != nullptr) {
        mem_free(uncompressed_buf);
    }
}

}  // end namespace storage