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

#ifndef COMPRESS_UNCOMPRESSED_COMPRESSOR_H
#define COMPRESS_UNCOMPRESSED_COMPRESSOR_H

#include <string.h>

#include "common/allocator/alloc_base.h"
#include "compressor.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

class UncompressedCompressor : public Compressor {
   public:
    UncompressedCompressor() : uncompressed_buf_(nullptr) {}
    virtual ~UncompressedCompressor() {
        if (uncompressed_buf_ != nullptr) {
            common::mem_free(uncompressed_buf_);
            uncompressed_buf_ = nullptr;
        }
    }
    int reset(bool for_compress) {
        UNUSED(for_compress);
        if (uncompressed_buf_ != nullptr) {
            common::mem_free(uncompressed_buf_);
            uncompressed_buf_ = nullptr;
        }
        return common::E_OK;
    }
    void destroy() {
        if (uncompressed_buf_ != nullptr) {
            common::mem_free(uncompressed_buf_);
            uncompressed_buf_ = nullptr;
        }
    }
    int compress(char* uncompressed_buf, uint32_t uncompressed_buf_len,
                 char*& compressed_buf, uint32_t& compressed_buf_len) {
        compressed_buf = uncompressed_buf;
        compressed_buf_len = uncompressed_buf_len;
        return common::E_OK;
    }
    void after_compress(char* compressed_buf) { UNUSED(compressed_buf); }

    int uncompress(char* compressed_buf, uint32_t compressed_buf_len,
                   char*& uncompressed_buf, uint32_t& uncompressed_buf_len) {
        // Allocate + copy rather than aliasing compressed_buf, even though the
        // "uncompressed" bytes equal the input.  Every caller and the leak
        // safety-net below assume the same ownership contract as the real
        // compressors: uncompress() returns a heap buffer released by
        // after_uncompress(), and cached in uncompressed_buf_ so
        // reset()/destroy()/the dtor can reclaim it when an error path (e.g. a
        // corrupted page that returns before after_uncompress() runs) would
        // otherwise leak it.  Aliasing would point uncompressed_buf_ into the
        // caller's shared page buffer, so those mem_free() calls would free a
        // mid-buffer pointer -> heap corruption / double free.  A zero-copy
        // fast path would need an explicit "not owned" flag in the contract.
        char* buf = static_cast<char*>(
            common::mem_alloc(compressed_buf_len, common::MOD_COMPRESSOR_OBJ));
        if (buf == nullptr) {
            return common::E_OOM;
        }
        memcpy(buf, compressed_buf, compressed_buf_len);
        uncompressed_buf = buf;
        uncompressed_buf_ = buf;
        uncompressed_buf_len = compressed_buf_len;
        return common::E_OK;
    }
    void after_uncompress(char* uncompressed_buf) {
        // Free the buffer the caller is releasing, not the most-recently
        // allocated one cached in uncompressed_buf_.  Two successive
        // uncompress() calls would overwrite uncompressed_buf_ with the
        // second allocation; after_uncompress(first) used to free that
        // second buffer (use-after-free for the still-live one) and leak
        // the first.
        if (uncompressed_buf == nullptr) return;
        common::mem_free(uncompressed_buf);
        if (uncompressed_buf_ == uncompressed_buf) {
            uncompressed_buf_ = nullptr;
        }
    }

   private:
    char* uncompressed_buf_;
};

}  // end namespace storage
#endif  // COMPRESS_UNCOMPRESSED_COMPRESSOR_H
