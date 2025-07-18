#ifndef FLOAT_SPRINTZ_DECODER_H
#define FLOAT_SPRINTZ_DECODER_H

#include <vector>
#include <cstdint>
#include <cstring>
#include <string>
#include <algorithm>

#include "common/allocator/byte_stream.h"
#include "encoding/fire.h"
#include "int32_packer.h"
#include "sprintz_decoder.h"

namespace storage {

class FloatSprintzDecoder : public SprintzDecoder {
public:
    FloatSprintzDecoder()
        : fire_pred_(2), predict_scheme_("delta") {
        current_buffer_.resize(block_size_ + 1);
        convert_buffer_.resize(block_size_);
        SprintzDecoder::reset();
        pre_bits_ = 0;
        current_value_ = 0.0f;
        current_count_ = 0;
        decode_size_ = 0;
        is_block_readed_ = false;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0.0f);
        std::fill(convert_buffer_.begin(), convert_buffer_.end(), 0);
        fire_pred_.reset();
    }

    ~FloatSprintzDecoder() override = default;

    void set_predict_method(const std::string& method) {
        predict_scheme_ = method;
    }

    void reset() override {
        SprintzDecoder::reset();
        pre_bits_ = 0;
        current_value_ = 0.0f;
        current_count_ = 0;
        decode_size_ = 0;
        is_block_readed_ = false;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0.0f);
        std::fill(convert_buffer_.begin(), convert_buffer_.end(), 0);
        fire_pred_.reset();
    }

    bool has_remaining(common::ByteStream& input) override {
        int min_length = sizeof(uint32_t) + 1;
        return (is_block_readed_ && current_count_ < decode_size_) ||
               input.remaining_size() >= min_length;
    }

    int read_float(float& ret_value, common::ByteStream& input) override {
        if (!is_block_readed_) {
            decode_block(input);
        }
        ret_value = current_buffer_[current_count_++];
        if (current_count_ == decode_size_) {
            is_block_readed_ = false;
            current_count_ = 0;
        }
        return common::E_OK;
    }

protected:
    void decode_block(common::ByteStream& input) override {
        // read header bitWidth
        common::SerializationUtil::read_int_little_endian_padded_on_bit_width(
            input, 1, bit_width_);
        // MSB indicates raw floats
        if ((bit_width_ & (1 << 7)) != 0) {
            decode_size_ = bit_width_ & ~(1 << 7);
            // fallback: full-precision floats
            SinglePrecisionDecoderV2 decoder;
            for (int i = 0; i < decode_size_; ++i) {
                decoder.read_float(current_buffer_[i], input);
            }
        } else {
            // packed block
            decode_size_ = block_size_ + 1;
            // read initial float bits
            common::SerializationUtil::read_int_little_endian_padded_on_bit_width(
                input, 32, pre_bits_);
            // convert bits to float
            std::memcpy(&current_buffer_[0], &pre_bits_, sizeof(pre_bits_));
            // read packed data
            std::vector<uint8_t> pack_buf(bit_width_);
            size_t read_len = 0;
            input.read_buf(reinterpret_cast<char*>(pack_buf.data()), bit_width_, read_len);
            packer_ = std::make_shared<Int32Packer>(bit_width_);
            std::vector<int32_t> tmp_buffer(block_size_);
            packer_->unpack_8values(pack_buf.data(), 0, tmp_buffer.data());
            // move into convert_buffer_
            for (int i = 0; i < block_size_; ++i) {
                convert_buffer_[i] = tmp_buffer[i];
            }
            recalculate();
        }
        is_block_readed_ = true;
    }

    void recalculate() override {
        // revert zigzag
        for (int i = 0; i < block_size_; ++i) {
            int32_t v = convert_buffer_[i];
            convert_buffer_[i] = (v % 2 == 0) ? -v / 2 : (v + 1) / 2;
        }
        if (predict_scheme_ == "delta") {
            // delta scheme
            for (int i = 0; i < block_size_; ++i) {
                uint32_t bits = static_cast<uint32_t>(convert_buffer_[i]) +
                                *reinterpret_cast<uint32_t*>(&current_buffer_[i]);
                float f;
                std::memcpy(&f, &bits, sizeof(bits));
                current_buffer_[i + 1] = f;
            }
        } else if (predict_scheme_ == "fire") {
            fire_pred_.reset();
            for (int i = 0; i < block_size_; ++i) {
                uint32_t prev_bits;
                std::memcpy(&prev_bits, &current_buffer_[i], sizeof(prev_bits));
                int32_t pred = fire_pred_.predict(prev_bits);
                int32_t err = convert_buffer_[i];
                int32_t corrected = pred + err;
                float f;
                std::memcpy(&f, &corrected, sizeof(corrected));
                current_buffer_[i + 1] = f;
                fire_pred_.train(prev_bits, corrected, err);
            }
        } else {
            // unsupported scheme
            ASSERT(false);
        }
    }

private:
    uint32_t pre_bits_;
    float current_value_;
    size_t current_count_;
    int decode_size_;
    bool is_block_readed_{false};

    std::vector<float> current_buffer_;
    std::vector<int32_t> convert_buffer_;
    std::shared_ptr<Int32Packer> packer_;
    IntFire fire_pred_;
    std::string predict_scheme_;
};

}  // namespace storage

#endif  // FLOAT_SPRINTZ_DECODER_H