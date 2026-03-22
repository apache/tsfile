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

#ifndef ENCODING_PLAIN_ENCODER_H
#define ENCODING_PLAIN_ENCODER_H

#include <cstring>

#include "encoder.h"

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define TSFILE_HAS_NEON 1
#endif

namespace storage {

class PlainEncoder : public Encoder {
   public:
    PlainEncoder() {}
    ~PlainEncoder() { destroy(); }
    void destroy() { /* do nothing for PlainEncoder */
    }
    void reset() { /* do thing for PlainEncoder */
    }

    FORCE_INLINE int encode(bool value, common::ByteStream& out_stream) {
        return common::SerializationUtil::write_i8(value ? 1 : 0, out_stream);
    }

    FORCE_INLINE int encode(int32_t value, common::ByteStream& out_stream) {
        return common::SerializationUtil::write_var_int(value, out_stream);
    }

    FORCE_INLINE int encode(int64_t value, common::ByteStream& out_stream) {
        return common::SerializationUtil::write_i64(value, out_stream);
    }

    FORCE_INLINE int encode(float value, common::ByteStream& out_stream) {
        return common::SerializationUtil::write_float(value, out_stream);
    }

    FORCE_INLINE int encode(double value, common::ByteStream& out_stream) {
        return common::SerializationUtil::write_double(value, out_stream);
    }

    FORCE_INLINE int encode(common::String value,
                            common::ByteStream& out_stream) {
        return common::SerializationUtil::write_mystring(value, out_stream);
    }

    int flush(common::ByteStream& out_stream) {
        // do nothing for PlainEncoder
        return common::E_OK;
    }

    int get_max_byte_size() { return 0; }

    // Optimized batch encoding: directly byte-swap into ByteStream page buffer.
    // Avoids per-value write_buf overhead entirely — only calls acquire_buf()
    // once per page boundary crossing.
    int encode_batch(const int64_t* values, uint32_t count,
                     common::ByteStream& out_stream) override {
        if (count == 0) return common::E_OK;
        uint32_t offset = 0;
        while (offset < count) {
            common::ByteStream::Buffer buf = out_stream.acquire_buf();
            if (UNLIKELY(buf.buf_ == nullptr)) return common::E_OOM;
            // How many int64 values fit in the remaining page space?
            uint32_t capacity = buf.len_ / 8;
            if (capacity == 0) {
                // Page has < 8 bytes left, fall back to write_buf for this one
                return Encoder::encode_batch(values + offset, count - offset,
                                             out_stream);
            }
            uint32_t batch = std::min(count - offset, capacity);
            uint8_t* dst = (uint8_t*)buf.buf_;
            const int64_t* src = values + offset;
            uint32_t i = 0;
#if TSFILE_HAS_NEON
            // NEON: byte-reverse 2 x int64 per iteration
            for (; i + 2 <= batch; i += 2) {
                uint8x16_t v = vld1q_u8((const uint8_t*)&src[i]);
                v = vrev64q_u8(v);
                vst1q_u8(dst, v);
                dst += 16;
            }
#endif
            // Scalar tail
            for (; i < batch; i++) {
                uint64_t v = (uint64_t)src[i];
                dst[0] = (uint8_t)(v >> 56);
                dst[1] = (uint8_t)(v >> 48);
                dst[2] = (uint8_t)(v >> 40);
                dst[3] = (uint8_t)(v >> 32);
                dst[4] = (uint8_t)(v >> 24);
                dst[5] = (uint8_t)(v >> 16);
                dst[6] = (uint8_t)(v >> 8);
                dst[7] = (uint8_t)(v);
                dst += 8;
            }
            out_stream.buffer_used(batch * 8);
            offset += batch;
        }
        return common::E_OK;
    }

    int encode_batch(const double* values, uint32_t count,
                     common::ByteStream& out_stream) override {
        return encode_batch(reinterpret_cast<const int64_t*>(values), count,
                            out_stream);
    }

    int encode_batch(const float* values, uint32_t count,
                     common::ByteStream& out_stream) override {
        if (count == 0) return common::E_OK;
        uint32_t offset = 0;
        while (offset < count) {
            common::ByteStream::Buffer buf = out_stream.acquire_buf();
            if (UNLIKELY(buf.buf_ == nullptr)) return common::E_OOM;
            uint32_t capacity = buf.len_ / 4;
            if (capacity == 0) {
                return Encoder::encode_batch(values + offset, count - offset,
                                             out_stream);
            }
            uint32_t batch = std::min(count - offset, capacity);
            uint8_t* dst = (uint8_t*)buf.buf_;
            const float* src = values + offset;
            uint32_t i = 0;
#if TSFILE_HAS_NEON
            // NEON: byte-reverse 4 x float (32-bit) per iteration
            for (; i + 4 <= batch; i += 4) {
                uint8x16_t v = vld1q_u8((const uint8_t*)&src[i]);
                v = vrev32q_u8(v);
                vst1q_u8(dst, v);
                dst += 16;
            }
#endif
            for (; i < batch; i++) {
                uint32_t v;
                memcpy(&v, &src[i], sizeof(float));
                dst[0] = (uint8_t)(v >> 24);
                dst[1] = (uint8_t)(v >> 16);
                dst[2] = (uint8_t)(v >> 8);
                dst[3] = (uint8_t)(v);
                dst += 4;
            }
            out_stream.buffer_used(batch * 4);
            offset += batch;
        }
        return common::E_OK;
    }
};

}  // end namespace storage
#endif  // ENCODING_PLAIN_ENCODER_H
