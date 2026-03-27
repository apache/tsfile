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

#ifndef ENCODING_DECODER_H
#define ENCODING_DECODER_H

#include "common/allocator/byte_stream.h"
#include "common/db_common.h"

namespace storage {

class Decoder {
   public:
    Decoder() {}
    virtual ~Decoder() {}
    virtual void reset() = 0;
    virtual bool has_remaining(const common::ByteStream& buffer) = 0;
    virtual int read_boolean(bool& ret_value, common::ByteStream& in) = 0;
    virtual int read_int32(int32_t& ret_value, common::ByteStream& in) = 0;
    virtual int read_int64(int64_t& ret_value, common::ByteStream& in) = 0;
    virtual int read_float(float& ret_value, common::ByteStream& in) = 0;
    virtual int read_double(double& ret_value, common::ByteStream& in) = 0;
    virtual int read_String(common::String& ret_value, common::PageArena& pa,
                            common::ByteStream& in) = 0;

    virtual int read_batch_int32(int32_t* out, int capacity, int& actual,
                                 common::ByteStream& in) {
        actual = 0;
        int ret = common::E_OK;
        int32_t val;
        while (actual < capacity && has_remaining(in)) {
            ret = read_int32(val, in);
            if (ret != common::E_OK) { return ret; }
            out[actual++] = val;
        }
        return common::E_OK;
    }

    virtual int read_batch_int64(int64_t* out, int capacity, int& actual,
                                 common::ByteStream& in) {
        actual = 0;
        int ret = common::E_OK;
        int64_t val;
        while (actual < capacity && has_remaining(in)) {
            ret = read_int64(val, in);
            if (ret != common::E_OK) { return ret; }
            out[actual++] = val;
        }
        return common::E_OK;
    }

    virtual int read_batch_float(float* out, int capacity, int& actual,
                                 common::ByteStream& in) {
        actual = 0;
        int ret = common::E_OK;
        float val;
        while (actual < capacity && has_remaining(in)) {
            ret = read_float(val, in);
            if (ret != common::E_OK) { return ret; }
            out[actual++] = val;
        }
        return common::E_OK;
    }

    virtual int read_batch_double(double* out, int capacity, int& actual,
                                  common::ByteStream& in) {
        actual = 0;
        int ret = common::E_OK;
        double val;
        while (actual < capacity && has_remaining(in)) {
            ret = read_double(val, in);
            if (ret != common::E_OK) { return ret; }
            out[actual++] = val;
        }
        return common::E_OK;
    }

    virtual int skip_int32(int count, int& skipped, common::ByteStream& in) {
        skipped = 0;
        int ret = common::E_OK;
        int32_t dummy;
        while (skipped < count && has_remaining(in)) {
            ret = read_int32(dummy, in);
            if (ret != common::E_OK) { return ret; }
            ++skipped;
        }
        return common::E_OK;
    }

    virtual int skip_int64(int count, int& skipped, common::ByteStream& in) {
        skipped = 0;
        int ret = common::E_OK;
        int64_t dummy;
        while (skipped < count && has_remaining(in)) {
            ret = read_int64(dummy, in);
            if (ret != common::E_OK) { return ret; }
            ++skipped;
        }
        return common::E_OK;
    }

    virtual int skip_float(int count, int& skipped, common::ByteStream& in) {
        skipped = 0;
        int ret = common::E_OK;
        float dummy;
        while (skipped < count && has_remaining(in)) {
            ret = read_float(dummy, in);
            if (ret != common::E_OK) { return ret; }
            ++skipped;
        }
        return common::E_OK;
    }

    virtual int skip_double(int count, int& skipped, common::ByteStream& in) {
        skipped = 0;
        int ret = common::E_OK;
        double dummy;
        while (skipped < count && has_remaining(in)) {
            ret = read_double(dummy, in);
            if (ret != common::E_OK) { return ret; }
            ++skipped;
        }
        return common::E_OK;
    }

    // Block-level filter check: peek the next block header and compute
    // the value range [block_min, block_max] without decoding.
    // Returns true if a block was peeked; false if not supported or no data.
    // After peeking, caller must either:
    //   - Call skip_peeked_block_int64() to skip the block
    //   - Call read_batch_int64() which will use the peeked header
    virtual bool peek_next_block_range_int64(common::ByteStream& in,
                                              int64_t& block_min,
                                              int64_t& block_max,
                                              int& block_count) {
        return false;
    }

    // Skip the block whose header was already consumed by peek.
    virtual int skip_peeked_block_int64(common::ByteStream& in, int& skipped) {
        return common::E_NOT_SUPPORT;
    }
};

}  // end namespace storage
#endif  // ENCODING_DECODER_H
