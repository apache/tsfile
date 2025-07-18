#ifndef FLOAT_SPRINTZ_ENCODER_H
#define FLOAT_SPRINTZ_ENCODER_H

#include <vector>
#include <cstdint>
#include <cstring>
#include <memory>

#include "common/allocator/byte_stream.h"
#include "encoding/encode_utils.h"
#include "encoding/int32_rle_encoder.h"
#include "encoding/fire.h"
#include "sprintz_encoder.h"
#include "int32_packer.h"

namespace storage {

class FloatSprintzEncoder : public SprintzEncoder {
public:
    FloatSprintzEncoder()
        : SprintzEncoder()
        , fire_pred_(2)
    {
        convert_buffer_.resize(block_size_);
    }

    ~FloatSprintzEncoder() override = default;

    void reset() override {
        SprintzEncoder::reset();
        values_.clear();
    }

    int encode(bool, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float value, common::ByteStream& out_stream) override {
        int ret = common::E_OK;
        if (!is_first_cached_) {
            values_.push_back(value);
            is_first_cached_ = true;
            return ret;
        }
        values_.push_back(value);

        if (values_.size() == block_size_ + 1) {
            float previous = values_[0];
            fire_pred_.reset();
            for (int i = 1; i <= block_size_; ++i) {
                convert_buffer_[i - 1] = predict(values_[i], values_[i - 1]);
            }
            bit_pack();
            is_first_cached_ = false;
            values_.clear();
            group_num_++;
            if (group_num_ == group_max_) {
                if (RET_FAIL(flush(out_stream))) return ret;
            }
        }
        return ret;
    }
    int encode(double, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(const common::String&, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int flush(common::ByteStream& out_stream) override {
        int ret = common::E_OK;
        if (byte_cache_.total_size() > 0) {
            if (RET_FAIL(
                common::SerializationUtil::chunk_read_all_data(byte_cache_, out_stream))) {
                return ret;
            }
        }

        if (!values_.empty()) {
            int size = static_cast<int>(values_.size());
            size |= (1 << 7);  // set MSB to indicate raw block
            common::SerializationUtil::write_int_little_endian_padded_on_bit_width(
                size, out_stream, 1);
            // fallback: encode remaining as full-precision floats
            for (float f : values_) {
                uint32_t bits;
                static_assert(sizeof(bits) == sizeof(f), "size mismatch");
                std::memcpy(&bits, &f, sizeof(f));
                common::SerializationUtil::write_int_little_endian_padded_on_bit_width(
                    bits, out_stream, 32);
            }
        }

        reset();
        return ret;
    }

protected:
    void bit_pack() override {
        // extract and remove first value
        uint32_t pre_bits;
        static_assert(sizeof(pre_bits) == sizeof(values_[0]), "size mismatch");
        std::memcpy(&pre_bits, &values_[0], sizeof(pre_bits));
        values_.erase(values_.begin());

        bit_width_ = get_int32_max_bit_width(convert_buffer_);
        packer_ = std::make_shared<Int32Packer>(bit_width_);

        std::vector<uint8_t> bytes(bit_width_);
        packer_->pack_8values(convert_buffer_.data(), 0, bytes.data());

        common::SerializationUtil::write_int_little_endian_padded_on_bit_width(
            bit_width_, byte_cache_, 1);
        common::SerializationUtil::write_int_little_endian_padded_on_bit_width(
            pre_bits, byte_cache_, 32);
        byte_cache_.write_buf(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    int32_t predict(float value, float prev_value) {
        uint32_t curr_bits, prev_bits;
        std::memcpy(&curr_bits, &value, sizeof(curr_bits));
        std::memcpy(&prev_bits, &prev_value, sizeof(prev_bits));
        int32_t raw_pred;
        if (predict_method_ == "delta") {
            raw_pred = static_cast<int32_t>(curr_bits - prev_bits);
        } else if (predict_method_ == "fire") {
            int32_t pred = fire_pred_.predict(prev_bits);
            int32_t err = static_cast<int32_t>(curr_bits) - pred;
            fire_pred_.train(prev_bits, static_cast<int32_t>(curr_bits), err);
            raw_pred = err;
        } else {
            // unsupported
            ASSERT(false);
            raw_pred = 0;
        }
        return (raw_pred <= 0) ? -2 * raw_pred : 2 * raw_pred - 1;
    }

private:
    std::vector<float> values_;
    std::vector<int32_t> convert_buffer_;
    std::shared_ptr<Int32Packer> packer_;
    IntFire fire_pred_;
};

}  // namespace storage

#endif  // FLOAT_SPRINTZ_ENCODER_H

