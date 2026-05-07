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

#ifndef ENCODING_SPRINTZ_OPT_INT64_CODEC_H
#define ENCODING_SPRINTZ_OPT_INT64_CODEC_H

#include <array>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <vector>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#include "common/allocator/byte_stream.h"
#include "encoding/bitpacking/int64_packer_n.h"
#include "encoding/decoder.h"
#include "encoding/encoder.h"
#include "encoding/fire.h"
#include "encoding/int64_rle_decoder.h"
#include "encoding/int64_rle_encoder.h"
#include "encoding/optimal/sprintz_optimal_pack_size.h"

namespace storage {

// A minimal C++ port of Java LongSprintzEncoder/LongSprintzDecoder optimal mode.
// - Predict scheme: "fire" by default (matches C++ Sprintz default), can be set to "delta".
// - Stream starts with [0][1] marker for optimal mode.
// - Each block: [packSize:1B][bitWidth:1B][preValue:8B][packed residuals]
// - Special cases:
//   - single value: [0][0][preValue:8B]
//   - legacy RLE block (for trailing in legacy mode): not used here.
class SprintzOptInt64Encoder final : public Encoder {
   public:
    static constexpr bool kNeonEnabled =
#if defined(__ARM_NEON)
        true;
#else
        false;
#endif

    explicit SprintzOptInt64Encoder(bool use_optimal_pack_size, int fixed_pack_size = 8,
                                   const std::string &predict_method = "delta",
                                   int optimal_chunk_min_size = 1024,
                                   bool enable_simd = false)
        : use_optimal_(use_optimal_pack_size),
          fixed_pack_size_(std::max(1, std::min(32, fixed_pack_size))),
          predict_method_(predict_method),
          optimal_chunk_min_size_(std::max(2, optimal_chunk_min_size)),
          enable_simd_(enable_simd),
          fire_pred_(3),
          packer_n_(1, enable_simd) {
        reset();
    }

    void reset() override {
        values_.clear();
        chunk_.clear();
        marker_written_ = false;
        bw_scratch_.clear();
        bw_hist_.fill(0);
        bw_hist_total_blocks_ = 0;
        collect_bw_stats_ = false;
        bw_stats_topk_ = 0;
        if (use_optimal_) {
            if (const char *env = std::getenv("TSFILE_OPT_BW_TOPK")) {
                int k = atoi(env);
                if (k > 0) {
                    collect_bw_stats_ = true;
                    bw_stats_topk_ = std::min(32, k);
                }
            }
        }
    }

    void destroy() override {}

    int encode(bool, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(int32_t, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(float, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(double, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(common::String, common::ByteStream &) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(int64_t value, common::ByteStream &out) override {
        // Buffer into chunk; encode on flush (whole-stream encoding for benchmarks/tests).
        chunk_.push_back(value);
        return common::E_OK;
    }

    int flush(common::ByteStream &out) override {
        if (!chunk_.empty()) {
            // Encode remaining chunk; if only 1 value, write single-value block.
            if ((int)chunk_.size() == 1) {
                write_marker_if_needed(out);
                // [0][0][preValue]
                common::SerializationUtil::write_ui8(0, out);
                common::SerializationUtil::write_ui8(0, out);
                common::SerializationUtil::write_ui64((uint64_t)chunk_[0], out);
                chunk_.clear();
            } else {
                int ret = encode_chunk(out);
                if (ret != common::E_OK) return ret;
            }
        }

        // Trailing legacy path for values_ is unused here; keep for parity.
        if (!values_.empty()) {
            // Encode any pending as RLE (rare). Format: [0][size] + RLE payload
            common::SerializationUtil::write_ui8(0, out);
            common::SerializationUtil::write_int_little_endian_padded_on_bit_width(
                (int32_t)values_.size(), out, 1);
            Int64RleEncoder rle;
            for (auto v : values_) {
                rle.encode(v, out);
            }
            rle.flush(out);
            values_.clear();
        }
        if (collect_bw_stats_ && bw_hist_total_blocks_ > 0) {
            print_bw_topk();
        }
        reset();
        return common::E_OK;
    }

    int get_max_byte_size() override { return (int)(1 + (chunk_.size() + values_.size() + 2) * 8); }

   private:
    static FORCE_INLINE uint64_t u64_max_upto_32_scalar(const int64_t *p, int n) {
        uint64_t m = 0;
        for (int i = 0; i < n; i++) m = std::max(m, (uint64_t)p[i]);
        return m;
    }

    static FORCE_INLINE uint64_t u64_max_upto_32_neon(const int64_t *p, int n) {
        // Best-effort SIMD for arm64; falls back to scalar.
        uint64_t m = 0;
#if defined(__ARM_NEON)
        int i = 0;
        uint64x2_t vmax2 = vdupq_n_u64(0);
        for (; i + 1 < n; i += 2) {
            // Treat residuals as unsigned magnitudes (they are non-negative after zigzag).
            uint64x2_t v = vld1q_u64(reinterpret_cast<const uint64_t *>(p + i));
            // Some toolchains don't expose vmaxq_u64; implement via compare+select.
            uint64x2_t mask = vcgtq_u64(v, vmax2);
            vmax2 = vbslq_u64(mask, v, vmax2);
        }
        uint64_t lanes[2];
        vst1q_u64(lanes, vmax2);
        m = std::max(lanes[0], lanes[1]);
        for (; i < n; i++) m = std::max(m, (uint64_t)p[i]);
#else
        for (int i = 0; i < n; i++) m = std::max(m, (uint64_t)p[i]);
#endif
        return m;
    }

    int encode_chunk(common::ByteStream &out) {
        if ((int)chunk_.size() < 2) {
            // defer to values_ for flush
            for (auto v : chunk_) values_.push_back(v);
            chunk_.clear();
            return common::E_OK;
        }
        write_marker_if_needed(out);

        const int n = (int)chunk_.size();
        // originals: use chunk_ itself.
        residuals_.resize((size_t)(n - 1));
        fire_pred_.reset();
        int64_t pre = chunk_[0];
        for (int i = 1; i < n; i++) {
            int64_t pred = (predict_method_ == "delta") ? (chunk_[i] - pre)
                                                        : fire_predict(chunk_[i], pre);
            residuals_[i - 1] = zigzag(pred);
            pre = chunk_[i];
        }

        int pack_size = use_optimal_
                            ? optimal::SprintzOptimalPackSize::find_optimal_pack_size(
                                  residuals_.data(),
                                  (int)residuals_.size(),
                                  &bw_scratch_,
                                  enable_simd_)
                            : fixed_pack_size_;
        pack_size = std::max(1, std::min(32, pack_size));

        const int rlen = (int)residuals_.size();
        const int num_packs = (rlen + pack_size - 1) / pack_size;

        packer_n_.set_width(1);

        for (int p = 0; p < num_packs; p++) {
            int start = p * pack_size;
            int end = std::min(start + pack_size, rlen);
            int actual_pack = end - start;
            // preValue is originals[start]
            int64_t block_pre = chunk_[start];

            uint64_t maxv = 0;
            if (enable_simd_) {
                maxv = u64_max_upto_32_neon(residuals_.data() + start, actual_pack);
            } else {
                maxv = u64_max_upto_32_scalar(residuals_.data() + start, actual_pack);
            }
            maxv = std::max<uint64_t>(1, maxv);
            int bit_width = 64 - __builtin_clzll(maxv);
            if (bit_width <= 0) bit_width = 1;
            if (collect_bw_stats_) {
                int bw = std::max(1, std::min(64, bit_width));
                bw_hist_[(size_t)bw]++;
                bw_hist_total_blocks_++;
            }

            int packed_bytes = (actual_pack * bit_width + 7) / 8;
            packer_n_.set_width(bit_width);
            // packed bytes upper bound for n<=32 and width<=64: 256 bytes
            packer_n_.pack_n_values(residuals_.data(), start, actual_pack, packed_scratch_);

            common::SerializationUtil::write_ui8((uint8_t)actual_pack, out);
            common::SerializationUtil::write_ui8((uint8_t)bit_width, out);
            common::SerializationUtil::write_ui64((uint64_t)block_pre, out);
            out.write_buf(packed_scratch_, (uint32_t)packed_bytes);
        }

        chunk_.clear();
        return common::E_OK;
    }

    void print_bw_topk() const {
        struct Pair {
            int bw;
            uint32_t cnt;
        };
        std::vector<Pair> v;
        v.reserve(64);
        for (int bw = 1; bw <= 64; bw++) {
            uint32_t c = bw_hist_[(size_t)bw];
            if (c) v.push_back(Pair{bw, c});
        }
        std::sort(v.begin(), v.end(), [](const Pair &a, const Pair &b) {
            if (a.cnt != b.cnt) return a.cnt > b.cnt;
            return a.bw < b.bw;
        });
        int k = std::min<int>(bw_stats_topk_, (int)v.size());
        std::cout << "[opt-bw-topk] blocks=" << (unsigned long long)bw_hist_total_blocks_
                  << " topk=" << k << "\n";
        for (int i = 0; i < k; i++) {
            double pct = 100.0 * (double)v[i].cnt / (double)bw_hist_total_blocks_;
            std::cout << "  bw=" << v[i].bw << " cnt=" << (unsigned long long)v[i].cnt
                      << " pct=" << pct << "\n";
        }
        std::cout.flush();
    }

    void write_marker_if_needed(common::ByteStream &out) {
        if (marker_written_) return;
        common::SerializationUtil::write_ui8(0, out);
        common::SerializationUtil::write_ui8(1, out);
        marker_written_ = true;
    }

    int64_t fire_predict(int64_t value, int64_t prev) {
        int64_t pred = fire_pred_.predict(prev);
        int64_t err = value - pred;
        fire_pred_.train(prev, value, err);
        return err;
    }

    static int64_t zigzag(int64_t pred) { return (pred <= 0) ? (-2 * pred) : (2 * pred - 1); }

   private:
    bool use_optimal_;
    int fixed_pack_size_;
    std::string predict_method_;
    int optimal_chunk_min_size_;
    bool enable_simd_;

    LongFire fire_pred_;
    std::vector<int64_t> values_;
    std::vector<int64_t> chunk_;
    bool marker_written_;
    std::vector<int> bw_scratch_;
    std::vector<int64_t> residuals_;
    uint8_t packed_scratch_[256];
    bitpacking::Int64PackerN packer_n_;

    // Optional light stats (Optimal only; enabled by env TSFILE_OPT_BW_TOPK)
    bool collect_bw_stats_ = false;
    int bw_stats_topk_ = 0;
    std::array<uint32_t, 65> bw_hist_{};
    uint64_t bw_hist_total_blocks_ = 0;
};

class SprintzOptInt64Decoder final : public Decoder {
   public:
    explicit SprintzOptInt64Decoder(const std::string &predict_method = "delta",
                                    bool enable_simd_unpack = true)
        : predict_method_(predict_method),
          unpacker_(1, enable_simd_unpack),
          fire_pred_(3) {
        reset();
    }

    void reset() override {
        determined_ = false;
        optimal_mode_ = false;
        current_.clear();
        idx_ = 0;
        fire_pred_.reset();
        emit_pre_value_ = true;
    }

    bool has_remaining(const common::ByteStream &buffer) override {
        return idx_ < current_.size() || buffer.has_remaining();
    }

    int read_boolean(bool &, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int read_int32(int32_t &, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int read_float(float &, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int read_double(double &, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int read_String(common::String &, common::PageArena &, common::ByteStream &) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_int64(int64_t &ret_value, common::ByteStream &in) override {
        if (idx_ >= current_.size()) {
            int ret = decode_next_block(in);
            if (ret != common::E_OK) return ret;
        }
        if (idx_ >= current_.size()) return common::E_NO_MORE_DATA;
        ret_value = current_[idx_++];
        return common::E_OK;
    }

   private:
    int decode_next_block(common::ByteStream &in) {
        if (!determined_) {
            // Peek first two bytes: if [0][1] -> optimal mode, else rewind not supported in ByteStream,
            // so we require optimal mode in this codec.
            uint8_t b0 = 0, b1 = 0;
            int ret = common::SerializationUtil::read_ui8(b0, in);
            if (ret != common::E_OK) return ret;
            ret = common::SerializationUtil::read_ui8(b1, in);
            if (ret != common::E_OK) return ret;
            optimal_mode_ = (b0 == 0 && b1 == 1);
            determined_ = true;
            if (!optimal_mode_) {
                return common::E_OUT_OF_RANGE;
            }
        }

        uint8_t pack_size_u8 = 0;
        int ret = common::SerializationUtil::read_ui8(pack_size_u8, in);
        if (ret != common::E_OK) return ret;
        int pack_size = (int)pack_size_u8;
        if (pack_size == 0) {
            uint8_t next = 0;
            ret = common::SerializationUtil::read_ui8(next, in);
            if (ret != common::E_OK) return ret;
            if (next == 0) {
                uint64_t pre = 0;
                ret = common::SerializationUtil::read_ui64(pre, in);
                if (ret != common::E_OK) return ret;
                current_.assign(1, (int64_t)pre);
                idx_ = 0;
                emit_pre_value_ = false;
                return common::E_OK;
            }
            // RLE block [0][size] + payload
            int size = (int)next;
            current_.resize(size);
            Int64RleDecoder rle;
            for (int i = 0; i < size; i++) {
                int64_t v;
                ret = rle.read_int64(v, in);
                if (ret != common::E_OK) return ret;
                current_[i] = v;
            }
            idx_ = 0;
            emit_pre_value_ = false;
            return common::E_OK;
        }

        pack_size = std::min(pack_size, 32);
        uint8_t bit_width_u8 = 0;
        ret = common::SerializationUtil::read_ui8(bit_width_u8, in);
        if (ret != common::E_OK) return ret;
        int32_t bit_width = (int32_t)bit_width_u8;
        uint64_t pre_u64 = 0;
        ret = common::SerializationUtil::read_ui64(pre_u64, in);
        if (ret != common::E_OK) return ret;
        int packed_bytes = (pack_size * bit_width + 7) / 8;
        if (packed_bytes > (int)sizeof(packed_scratch_) || pack_size > 32) {
            return common::E_OUT_OF_RANGE;
        }
        uint32_t packed_read = 0;
        ret = in.read_buf(packed_scratch_, (uint32_t)packed_bytes, packed_read);
        if (ret != common::E_OK) return ret;
        if (packed_read != (uint32_t)packed_bytes) {
            return common::E_PARTIAL_READ;
        }

        unpacker_.set_width(bit_width);
        unpacker_.unpack_n_values(packed_scratch_, 0, pack_size, pack_residuals_);

        // reconstruct originals: recon[0]=pre, then zigzag inverse + predictor (match encoder).
        recon_vals_[0] = (int64_t)pre_u64;
        if (predict_method_ == "delta") {
            for (int i = 0; i < pack_size; i++) {
                uint64_t z = (uint64_t)pack_residuals_[i];
                int64_t e = (z % 2 == 0) ? -(int64_t)(z / 2) : (int64_t)((z + 1) / 2);
                recon_vals_[i + 1] = recon_vals_[i] + e;
            }
        } else {
            fire_pred_.reset();
            for (int i = 0; i < pack_size; i++) {
                uint64_t z = (uint64_t)pack_residuals_[i];
                int64_t e = (z % 2 == 0) ? -(int64_t)(z / 2) : (int64_t)((z + 1) / 2);
                int64_t pred = fire_pred_.predict(recon_vals_[i]);
                int64_t v = pred + e;
                recon_vals_[i + 1] = v;
                fire_pred_.train(recon_vals_[i], v, e);
            }
        }

        if (emit_pre_value_) {
            current_.resize((size_t)pack_size + 1);
            std::memcpy(current_.data(), recon_vals_,
                        sizeof(int64_t) * (size_t)(pack_size + 1));
            emit_pre_value_ = false;
        } else {
            current_.resize((size_t)pack_size);
            std::memcpy(current_.data(), recon_vals_ + 1,
                        sizeof(int64_t) * (size_t)pack_size);
        }
        idx_ = 0;
        return common::E_OK;
    }

   private:
    std::string predict_method_;
    bitpacking::Int64PackerN unpacker_;
    LongFire fire_pred_;

    uint8_t packed_scratch_[256];
    int64_t pack_residuals_[32];
    int64_t recon_vals_[33];

    bool determined_;
    bool optimal_mode_;
    std::vector<int64_t> current_;
    size_t idx_;
    bool emit_pre_value_;
};

}  // namespace storage

#endif  // ENCODING_SPRINTZ_OPT_INT64_CODEC_H

