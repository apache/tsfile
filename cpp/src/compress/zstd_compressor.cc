/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include "zstd_compressor.h"

#include <limits>

#include "common/allocator/alloc_base.h"

using namespace common;

namespace storage {

int ZstdCompressor::reset(bool /* for_compress */) { return E_OK; }

void ZstdCompressor::destroy() {
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }
    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

int ZstdCompressor::compress(char* uncompressed_buf,
                             uint32_t uncompressed_buf_len,
                             char*& compressed_buf,
                             uint32_t& compressed_buf_len) {
    const size_t max_dst_size = ZSTD_compressBound(uncompressed_buf_len);
    if (max_dst_size > std::numeric_limits<uint32_t>::max()) {
        return E_COMPRESS_ERR;
    }

    compressed_buf_ =
        static_cast<char*>(mem_alloc(max_dst_size, MOD_COMPRESSOR_OBJ));
    if (compressed_buf_ == nullptr) {
        return E_OOM;
    }

    const size_t compressed_data_size =
        ZSTD_compress(compressed_buf_, max_dst_size, uncompressed_buf,
                      uncompressed_buf_len, ZSTD_CLEVEL_DEFAULT);
    if (ZSTD_isError(compressed_data_size)) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
        return E_COMPRESS_ERR;
    }

    char* compressed_data =
        static_cast<char*>(mem_realloc(compressed_buf_, compressed_data_size));
    if (compressed_data == nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
        return E_OOM;
    }

    compressed_buf_ = compressed_data;
    compressed_buf = compressed_data;
    compressed_buf_len = static_cast<uint32_t>(compressed_data_size);
    return E_OK;
}

void ZstdCompressor::after_compress(char* compressed_buf) {
    if (compressed_buf != nullptr) {
        mem_free(compressed_buf);
        if (compressed_buf_ == compressed_buf) {
            compressed_buf_ = nullptr;
        }
    }
}

int ZstdCompressor::uncompress(char* compressed_buf,
                               uint32_t compressed_buf_len,
                               char*& uncompressed_buf,
                               uint32_t& uncompressed_buf_len) {
    const unsigned long long content_size =
        ZSTD_getFrameContentSize(compressed_buf, compressed_buf_len);
    if (content_size == ZSTD_CONTENTSIZE_ERROR ||
        content_size == ZSTD_CONTENTSIZE_UNKNOWN ||
        content_size > std::numeric_limits<uint32_t>::max()) {
        return E_COMPRESS_ERR;
    }

    const size_t alloc_size =
        content_size == 0 ? 1 : static_cast<size_t>(content_size);
    uncompressed_buf_ =
        static_cast<char*>(mem_alloc(alloc_size, MOD_COMPRESSOR_OBJ));
    if (uncompressed_buf_ == nullptr) {
        return E_OOM;
    }

    const size_t decompressed_size =
        ZSTD_decompress(uncompressed_buf_, static_cast<size_t>(content_size),
                        compressed_buf, compressed_buf_len);
    if (ZSTD_isError(decompressed_size) || decompressed_size != content_size) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
        return E_COMPRESS_ERR;
    }

    uncompressed_buf = uncompressed_buf_;
    uncompressed_buf_len = static_cast<uint32_t>(decompressed_size);
    return E_OK;
}

void ZstdCompressor::after_uncompress(char* uncompressed_buf) {
    if (uncompressed_buf != nullptr) {
        mem_free(uncompressed_buf);
        if (uncompressed_buf_ == uncompressed_buf) {
            uncompressed_buf_ = nullptr;
        }
    }
}

}  // namespace storage
