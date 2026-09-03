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

#include "lzma2_compressor.h"

#include <limits>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"

using namespace common;

namespace storage {

namespace {
const uint32_t LZMA2_BUFFER_SIZE = 4096;
}

int LZMA2Compressor::reset(bool /* for_compress */) { return E_OK; }

void LZMA2Compressor::destroy() {
    if (compressed_buf_ != nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
    }
    if (uncompressed_buf_ != nullptr) {
        mem_free(uncompressed_buf_);
        uncompressed_buf_ = nullptr;
    }
}

int LZMA2Compressor::compress(char* uncompressed_buf,
                              uint32_t uncompressed_buf_len,
                              char*& compressed_buf,
                              uint32_t& compressed_buf_len) {
    const size_t max_dst_size = lzma_stream_buffer_bound(uncompressed_buf_len);
    if (max_dst_size == 0 ||
        max_dst_size > std::numeric_limits<uint32_t>::max()) {
        return E_COMPRESS_ERR;
    }

    compressed_buf_ =
        static_cast<char*>(mem_alloc(max_dst_size, MOD_COMPRESSOR_OBJ));
    if (compressed_buf_ == nullptr) {
        return E_OOM;
    }

    size_t out_pos = 0;
    const lzma_ret ret = lzma_easy_buffer_encode(
        LZMA_PRESET_DEFAULT, LZMA_CHECK_CRC64, nullptr,
        reinterpret_cast<const uint8_t*>(uncompressed_buf),
        uncompressed_buf_len, reinterpret_cast<uint8_t*>(compressed_buf_),
        &out_pos, max_dst_size);
    if (ret != LZMA_OK) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
        return E_COMPRESS_ERR;
    }

    char* compressed_data =
        static_cast<char*>(mem_realloc(compressed_buf_, out_pos));
    if (compressed_data == nullptr) {
        mem_free(compressed_buf_);
        compressed_buf_ = nullptr;
        return E_OOM;
    }

    compressed_buf_ = compressed_data;
    compressed_buf = compressed_data;
    compressed_buf_len = static_cast<uint32_t>(out_pos);
    return E_OK;
}

void LZMA2Compressor::after_compress(char* compressed_buf) {
    if (compressed_buf != nullptr) {
        mem_free(compressed_buf);
        if (compressed_buf_ == compressed_buf) {
            compressed_buf_ = nullptr;
        }
    }
}

int LZMA2Compressor::uncompress(char* compressed_buf,
                                uint32_t compressed_buf_len,
                                char*& uncompressed_buf,
                                uint32_t& uncompressed_buf_len) {
    lzma_stream stream = LZMA_STREAM_INIT;
    lzma_ret ret = lzma_stream_decoder(&stream, UINT64_MAX, 0);
    if (ret != LZMA_OK) {
        return E_COMPRESS_ERR;
    }

    ByteStream out(LZMA2_BUFFER_SIZE, MOD_COMPRESSOR_OBJ);
    uint8_t out_buf[LZMA2_BUFFER_SIZE];
    stream.next_in = reinterpret_cast<const uint8_t*>(compressed_buf);
    stream.avail_in = compressed_buf_len;

    do {
        stream.next_out = out_buf;
        stream.avail_out = LZMA2_BUFFER_SIZE;

        ret = lzma_code(&stream, LZMA_FINISH);
        if (ret != LZMA_OK && ret != LZMA_STREAM_END) {
            lzma_end(&stream);
            out.destroy();
            return E_COMPRESS_ERR;
        }

        const uint32_t produced =
            static_cast<uint32_t>(LZMA2_BUFFER_SIZE - stream.avail_out);
        if (produced > 0 && out.write_buf(out_buf, produced) != E_OK) {
            lzma_end(&stream);
            out.destroy();
            return E_COMPRESS_ERR;
        }
    } while (ret != LZMA_STREAM_END);

    const size_t remaining_input = stream.avail_in;
    lzma_end(&stream);

    if (remaining_input != 0 ||
        out.total_size() > std::numeric_limits<uint32_t>::max()) {
        out.destroy();
        return E_COMPRESS_ERR;
    }

    uncompressed_buf_ = get_bytes_from_bytestream(out);
    uncompressed_buf_len = static_cast<uint32_t>(out.total_size());
    out.destroy();
    if (uncompressed_buf_len != 0 && uncompressed_buf_ == nullptr) {
        return E_OOM;
    }

    uncompressed_buf = uncompressed_buf_;
    return E_OK;
}

void LZMA2Compressor::after_uncompress(char* uncompressed_buf) {
    if (uncompressed_buf != nullptr) {
        mem_free(uncompressed_buf);
        if (uncompressed_buf_ == uncompressed_buf) {
            uncompressed_buf_ = nullptr;
        }
    }
}

}  // namespace storage
