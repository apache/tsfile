//
// Created by 李烁麟 on 25-8-4.
//

#ifndef SIMD_TS_2DIFF_H
#define SIMD_TS_2DIFF_H

#include <cstdint>
#include <cstring>
#include <iostream>

#include "simde/x86/avx2.h"
#include "utils.h"

static int N = 128;

enum class predict_type {
    EQ,
    GT,
    GE,
    LE,
    LT,
    BT,
};

enum class block_action { All, None, NeedScan };

struct predictor {
    predict_type type_;
    int32_t value_;
    int32_t rvalue_;

    bool satisfy(int32_t value) {
        switch (type_) {
            case predict_type::EQ:
                return value_ == value;
            case predict_type::GT:
                return value > value_;
            case predict_type::GE:
                return value >= value_;
            case predict_type::LT:
                return value < value_;
            case predict_type::LE:
                return value <= value_;
            case predict_type::BT:
                return value >= value_ && value <= rvalue_;
            default:
                return false;
        }
    }

    block_action block_check(int32_t lvalue, int32_t rvalue) {
        switch (type_) {
            case predict_type::LT: {
                if (lvalue >= value_) return block_action::None;
                if (rvalue < value_) return block_action::All;
                return block_action::NeedScan;
            }
            case predict_type::LE: {
                if (lvalue > value_) return block_action::None;
                if (rvalue <= value_) return block_action::All;
                return block_action::NeedScan;
            }
            case predict_type::GT: {
                if (rvalue <= value_) return block_action::None;
                if (lvalue > value_) return block_action::All;
                return block_action::NeedScan;
            }
            case predict_type::GE: {
                if (rvalue < value_) return block_action::None;
                if (lvalue >= value_) return block_action::All;
                return block_action::NeedScan;
            }
            case predict_type::EQ: {
                if (rvalue < value_ || lvalue > value_)
                    return block_action::None;
                if (lvalue <= value_ && rvalue >= value_)
                    return block_action::NeedScan;
                return block_action::NeedScan;
            }
        }
        return block_action::NeedScan;
    }
};

class simd_ts_2diff_decoder {
   public:
    simd_ts_2diff_decoder(uint8_t *data_array, int32_t data_size,
                          predictor *predict = nullptr)
        : data_array_(data_array), data_size_(data_size), predict_(predict) {
        bit_width_ = 0;
        write_index_ = 0;
        delta_min_ = 0;
        previous_value_ = 0;
        position_ = 0;
        bits_left_ = 0;
    };

    bool decode(int32_t *out, int32_t *offset_map, int &size) {
        if (position_ < data_size_) {
            position_ += handle_one_block(data_array_ + position_, out,
                                          offset_map, size);
            return true;
        } else {
            return false;
        }
    }

    static inline void drain_batch_compact(const int32_t out_u32[8],
                                           uint8_t mask, uint32_t position,
                                           std::vector<int32_t> &values,
                                           std::vector<int32_t> &offsets) {
        if (!mask) return;
        const int k = __builtin_popcount(mask);
        values.reserve(values.size() + k);
        offsets.reserve(offsets.size() + k);
        int32_t tmp_vals[8];
        int32_t tmp_offs[8];
        int n = 0;
        uint32_t m = mask;
        while (m) {
            int j = __builtin_ctz(m);
            m &= (m - 1);
            tmp_vals[n] = out_u32[j];
            tmp_offs[n] = position + (uint32_t)j + 1;
            ++n;
        }
        values.insert(values.end(), tmp_vals, tmp_vals + n);
        offsets.insert(offsets.end(), tmp_offs, tmp_offs + n);
    }

    size_t handle_one_block(uint8_t *data_array, int32_t *value,
                            int32_t *offset_map, int &size) {
        uint8_t *in = data_array;
        write_index_ = read_ui32(in);
        bit_width_ = read_ui32(in);
        delta_min_ = read_ui32(in);
        previous_value_ = read_ui32(in);
        int32_t block_min_lower =
            previous_value_ + (delta_min_ >= 0 ? 0 : N) * delta_min_;
        int32_t delta_upper_bound = (1 << bit_width_) - 1;
        int32_t max_delta = delta_min_ + delta_upper_bound;
        int32_t block_max_upper = previous_value_ +
                                  delta_min_ * (max_delta > 0 ? 1 : 0) +
                                  (max_delta > 0 ? N : 0) * delta_upper_bound;
        block_action action =
            predict_ == nullptr
                ? block_action::All
                : predict_->block_check(block_min_lower, block_max_upper);
        if (action == block_action::None) {
            return sizeof(write_index_) + sizeof(bit_width_) +
                   sizeof(delta_min_) + sizeof(previous_value_) +
                   (bit_width_ * write_index_ + 7) / 8;
        }

        if (predict_ != nullptr) {
            if (predict_->satisfy(previous_value_)) {
                value[size++] = previous_value_;
                offset_map[0] = 0;
            }
        } else {
            value[size++] = previous_value_;
            offset_map[0] = 0;
        }

        for (int i = 0; i < write_index_;) {
            if (!can_simd_decode8(i, in, data_array + data_size_)) {
                const int remain_data = write_index_ - i;
                previous_value_ =
                    handle_normal_data(in, previous_value_, remain_data, i,
                                       value, offset_map, size);
                i += remain_data;
            } else {
                int32_t out_u32[8];
                uint8_t mask;
                previous_value_ =
                    handle_batch_data(in, bit_width_, delta_min_, i,
                                      previous_value_, out_u32, mask);
                if (mask != 0) {
                    if(mask == 0xFF) {
                        memcpy(value +size, out_u32, 8 * sizeof(int32_t));
                        for (int k = 0; k < 8;  ++k) offset_map[size + k] = i + k + 1;
                        size += 8;
                    } else {
                        uint8_t m = mask;
                        while(m) {
                            int tz = __builtin_ctz((unsigned )m);
                            value[size] = out_u32[tz];
                            offset_map[size] = tz + i + 1;
                            ++ size;
                            m &= m -1;
                        }
                    }
                }
                i += 8;
            }
        }
        return sizeof(write_index_) + sizeof(bit_width_) + sizeof(delta_min_) +
               sizeof(previous_value_) + (bit_width_ * write_index_ + 7) / 8;
    }

    int32_t handle_normal_data(uint8_t *&in, int32_t base, int cap,
                               int position, int32_t *out, int32_t *offset,
                               int &size) {
        int32_t basement = base;
        for (int i = 0; i < cap; ++i) {
            int32_t value = read_long(bit_width_, in);
            value += (basement + delta_min_);
            basement = value;
            if (predict_ != nullptr) {
                if (predict_->satisfy(value)) {
                    out[size] = value;
                    offset[size++] = position + i + 1;
                }
            } else {
                out[size] = value;
                offset[size++] = position + i + 1;
            }
        }
        return basement;
    }

    int32_t read_long(int bits, uint8_t *&in) {
        int32_t value = 0;
        while (bits > 0) {
            read_byte_if_empty(in);
            if (bits > bits_left_ || bits == 8) {
                auto d = (uint8_t)(buffer_ & ((1 << bits_left_) - 1));
                value = (value << bits_left_) + (d & 0xFF);
                bits -= bits_left_;
                bits_left_ = 0;
            } else {
                auto d = (uint8_t)((((uint8_t)buffer_) >> (bits_left_ - bits)) &
                                   ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bits_left_ -= bits;
                bits = 0;
            }
            if (bits <= 0) {
                break;
            }
        }
        return value;
    }

    void read_byte_if_empty(uint8_t *&in) {
        if (bits_left_ == 0) {
            memcpy(&buffer_, in, 1);
            bits_left_ = 8;
            in += 1;
        }
    }

    bool can_simd_decode8(int pos, const uint8_t* cur, const uint8_t * end) {
        if (write_index_ - pos < 8) {
            return false;
        }
        const int32_t overhead_bits = (bit_width_ >= 16) ? 64 : 32;
        const int32_t rest_data = 8;
        const int32_t need_bits = (rest_data - 1) * bit_width_ + overhead_bits;
        const size_t  remain_bits = size_t(end - cur) * 8;
        return need_bits <= remain_bits;
    }

    int32_t handle_batch_data(const uint8_t *in, int32_t w, int32_t min_delta,
                              int32_t ind, int32_t base, int32_t out_u32[8],
                              uint8_t &mask) {
        // This function decodes 8 values from the input data array,
        // producing the final results that satisfy the prediction.

        // Set byte-reversal method to convert little-endian data to big-endian
        // format.
        static const simde__m256i SHUF_REV8 = simde_mm256_setr_epi8(
            7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3,
            2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);

        static const simde__m128i SHUF_REV4 = simde_mm_setr_epi8(
            3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12);

        const simde__m128i VMIN4 = simde_mm_set1_epi32(min_delta);

        int32_t basement = base;
        mask = 0;

        // Decode using two loops.
        for (int32_t grp = 0; grp < 2; ++grp) {
            int32_t pos0 = grp * 4u * w + ind * w;
            int32_t pos[4] = {pos0 + 0 * w, pos0 + 1 * w, pos0 + 2 * w,
                              pos0 + 3 * w};
            // The byte items start
            int32_t bidx_s[4] = {pos[0] >> 3, pos[1] >> 3, pos[2] >> 3,
                                 pos[3] >> 3};
            // The offset items start
            int32_t off_s[4] = {pos[0] & 7, pos[1] & 7, pos[2] & 7, pos[3] & 7};

            simde__m128i IDX128 =
                simde_mm_setr_epi32(bidx_s[0], bidx_s[1], bidx_s[2], bidx_s[3]);
            simde__m128i OFF128 =
                simde_mm_setr_epi32(off_s[0], off_s[1], off_s[2], off_s[3]);

            simde__m128i V4;

            if (w <= 16) {
                const int rshift = 32 - w;
                simde__m128i w32_le =
                    simde_mm_i32gather_epi32((const int *)in, IDX128, 1);
                simde__m128i w32_be = simde_mm_shuffle_epi8(w32_le, SHUF_REV4);
                simde__m128i U32 = simde_mm_sllv_epi32(w32_be, OFF128);
                simde__m128i RS32 = simde_mm_set1_epi32(rshift);
                simde__m128i V32 = simde_mm_srlv_epi32(U32, RS32);
                V4 = V32;
            } else {
                const int rshift = 64 - w;
                simde__m256i w64_le = simde_mm256_i32gather_epi64(
                    (const long long *)in, IDX128, 1);
                simde__m256i w64_be =
                    simde_mm256_shuffle_epi8(w64_le, SHUF_REV8);
                simde__m256i OFF64 = simde_mm256_cvtepu32_epi64(OFF128);
                simde__m256i U64 = simde_mm256_sllv_epi64(w64_be, OFF64);
                simde__m256i V64 =
                    simde_mm256_srl_epi64(U64, simde_mm_cvtsi32_si128(rshift));
                simde__m256i V32_8 = V64;
                simde__m256i perm =
                    simde_mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0);
                simde__m256i comp =
                    simde_mm256_permutevar8x32_epi32(V32_8, perm);
                V4 = simde_mm256_castsi256_si128(comp);
            }
            V4 = simde_mm_add_epi32(V4, VMIN4);
            simde__m128i t;
            t = simde_mm_slli_si128(V4, 4);
            V4 = simde_mm_add_epi32(V4, t);
            t = simde_mm_slli_si128(V4, 8);
            V4 = simde_mm_add_epi32(V4, t);
            simde__m128i C4 = simde_mm_set1_epi32((int)basement);
            V4 = simde_mm_add_epi32(V4, C4);
            if (predict_ != nullptr) {
                simde__m128i m4;
                simde__m128i a = simde_mm_set1_epi32(predict_->value_);
                switch (predict_->type_) {
                    case predict_type::EQ: {
                        m4 = simde_mm_cmpeq_epi32(V4, a);
                        break;
                    }
                    case predict_type::GT: {
                        m4 = simde_mm_cmpgt_epi32(V4, a);
                        break;
                    }
                    case predict_type::GE: {
                        m4 = simde_mm_xor_si128(simde_mm_cmpgt_epi32(a, V4),
                                                simde_mm_set1_epi32(-1));
                        break;
                    }
                    case predict_type::LT: {
                        m4 = simde_mm_cmplt_epi32(V4, a);
                        break;
                    }
                    case predict_type::LE: {
                        m4 = simde_mm_xor_si128(simde_mm_cmpgt_epi32(V4, a),
                                                simde_mm_set1_epi32(-1));
                        break;
                    }
                    case predict_type::BT: {
                        int32_t lo = predict_->value_;
                        int32_t hi = predict_->rvalue_;
                        simde__m128i A = simde_mm_set1_epi32(lo);
                        simde__m128i B = simde_mm_set1_epi32(hi);
                        simde__m128i left =
                            simde_mm_xor_si128(simde_mm_cmpgt_epi32(A, V4),
                                               simde_mm_set1_epi32(-1));
                        simde__m128i right =
                            simde_mm_xor_si128(simde_mm_cmpgt_epi32(V4, B),
                                               simde_mm_set1_epi32(-1));
                        m4 = simde_mm_and_si128(left, right);
                        break;
                    }
                    default:
                        break;
                }
                uint8_t four_bits = 0;
                four_bits =
                    simde_mm_movemask_ps(simde_mm_castsi128_ps(m4)) & 0xf;
                mask |= (uint8_t)(four_bits << (grp * 4));
            } else {
                mask = 0xff;
            }

            simde_mm_storeu_si128((simde__m128i *)(out_u32 + grp * 4), V4);
            basement = out_u32[grp * 4 + 3];
        }
        return basement;
    }

   private:
    uint8_t *data_array_;
    int bit_width_;
    int write_index_;
    int32_t data_size_;
    int32_t position_;
    int32_t delta_min_;
    int32_t previous_value_;
    predictor *predict_;

    uint8_t buffer_;
    int bits_left_;
};

#endif  // SIMD_TS_2DIFF_H
