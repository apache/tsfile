// double_sprintz_encoder.h
#ifndef DOUBLE_SPRINTZ_ENCODER_H
#define DOUBLE_SPRINTZ_ENCODER_H

#include <vector>
#include <cstdint>
#include <cstring>                  // for std::memcpy
#include "sprintz_encoder.h"
#include "encoding/fire.h"
#include "encoding/int64_packer.h"
#include "encoding/int64_rle_encoder.h"   // reuse RLE for tail
#include "common/allocator/byte_stream.h"
#include "encoding/encode_utils.h"         // for get_int64_max_bit_width

namespace storage {

class DoubleSprintzEncoder : public SprintzEncoder {
public:
    DoubleSprintzEncoder()
      : SprintzEncoder(), fire_pred_(3) {}

    ~DoubleSprintzEncoder() override = default;

    void reset() override {
        SprintzEncoder::reset();
        values_.clear();
    }

    void destroy() override {
        // nothing to clean up
    }

    // Only support double
    int encode(int64_t, common::ByteStream&) override { return common::E_TYPE_NOT_MATCH; }
    int encode(int32_t, common::ByteStream&) override { return common::E_TYPE_NOT_MATCH; }
    int encode(float,   common::ByteStream&) override { return common::E_TYPE_NOT_MATCH; }
    int encode(bool,    common::ByteStream&) override { return common::E_TYPE_NOT_MATCH; }
    int encode(const common::String&, common::ByteStream&) override { return common::E_TYPE_NOT_MATCH; }

    // Main encode for double
    int encode(double value, common::ByteStream& out_stream) override {
        if (!is_first_cached_) {
            values_.push_back(value);
            is_first_cached_ = true;
            return common::E_OK;
        }

        values_.push_back(value);
        if (values_.size() == block_size_ + 1) {
            // full block: compute predictions and bit-pack
            double first = values_[0];
            fire_pred_.reset();

            // build the converted error buffer
            std::vector<int64_t> errs(block_size_);
            int64_t prev_bits = toBits(first);
            for (int i = 1; i <= block_size_; ++i) {
                int64_t curr_bits = toBits(values_[i]);
                int64_t pred = (predict_method_ == "delta")
                    ? delta(curr_bits, prev_bits)
                    : fire(curr_bits, prev_bits);
                // zig-zag
                errs[i-1] = (pred <= 0) ? -2 * pred : 2 * pred - 1;
                prev_bits = curr_bits;
            }

            // bit-pack and emit
            bit_pack(errs, first);
            is_first_cached_ = false;
            values_.clear();
            if (++group_num_ == group_max_) {
                flush(out_stream);
            }
        }
        return common::E_OK;
    }

    int flush(common::ByteStream& out_stream) override {
        // first, dump any full‐block cache
        if (byte_cache_.total_size() > 0) {
            common::SerializationUtil::chunk_read_all_data(byte_cache_, out_stream);
        }

        // then RLE‐encode the tail of any leftover values
        if (!values_.empty()) {
            int size = static_cast<int>(values_.size());
            size |= (1 << 7);
            common::SerializationUtil::write_int_little_endian_padded_on_bit_width(size, out_stream, 1);

            Int64RleEncoder rle;  // reuse for 64‑bit values
            for (double v : values_) {
                rle.encode(toBits(v), out_stream);
            }
            rle.flush(out_stream);
        }

        reset();
        return common::E_OK;
    }

    int get_one_item_max_size() override {
        return 1 + (1 + block_size_) * sizeof(double);
    }

    int get_max_byte_size() override {
        return 1 + (values_.size() + 1) * sizeof(double);
    }

protected:
    void bit_pack(const std::vector<int64_t>& errs, double first_value) {
        // determine bit width
        bit_width_ = get_int64_max_bit_width(errs);
        packer_ = std::make_shared<Int64Packer>(bit_width_);

        // pack 8 errors into bytes
        std::vector<uint8_t> packed((bit_width_ + 7) / 8 * block_size_ / 8);
        packer_->pack_8values(errs.data(), 0, packed.data());

        // write bit-width header
        common::SerializationUtil::write_int_little_endian_padded_on_bit_width(bit_width_, byte_cache_, 1);

        // write the “first” double as 8 raw bytes, little-endian
        uint64_t bits = toBits(first_value);
        for (int i = 0; i < 8; ++i) {
            byte_cache_.write_buf(reinterpret_cast<const char*>(&(reinterpret_cast<const uint8_t*>(&bits)[i])), 1);
        }

        // write the packed error bytes
        byte_cache_.write_buf(reinterpret_cast<const char*>(packed.data()), packed.size());
    }

    static int64_t toBits(double v) {
        int64_t bits;
        std::memcpy(&bits, &v, sizeof(bits));
        return bits;
    }

    static int64_t delta(int64_t curr, int64_t prev) {
        return curr - prev;
    }
    static int64_t fire(int64_t curr, int64_t prev) {
        return curr - fire_pred_.predict(prev);
    }

private:
    std::vector<double> values_;
    std::shared_ptr<Int64Packer> packer_;
    LongFire fire_pred_;
};

} // namespace storage

#endif // DOUBLE_SPRINTZ_ENCODER_H
