//
// Created by 李烁麟 on 25-8-3.
//

#ifndef TS_2DIFF_H
#define TS_2DIFF_H

#include <cstdint>
#include <cstring>
#include <iostream>

#include "utils.h"

template <typename>
struct always_false : std::false_type {};

template <typename T,
          typename =
              typename std::enable_if<std::is_same<T, int32_t>::value ||
                                      std::is_same<T, int64_t>::value>::type>
class ts_2diff_encoder {
   public:
    ts_2diff_encoder() {
        block_size_ = 128;
        delta_array_ = new T[block_size_];
        write_index_ = -1;
        bits_left_ = 8;
        buffer_ = 0;
        delta_array_min_ = 0;
        delta_array_max_ = 0;
        first_value_ = 0;
        previous_value_ = 0;
        cap_ = 1024;
        cur_position_ = 0;
        data_array_ = (uint8_t*)malloc(cap_);
    }

    ~ts_2diff_encoder() { free(data_array_); }

    void do_encode(T value) {
        // Record first value.
        if (write_index_ == -1) {
            first_value_ = value;
            previous_value_ = first_value_;
            write_index_++;
            return;
        }

        T delta = value - previous_value_;
        previous_value_ = value;

        if (write_index_ == 0) {
            delta_array_min_ = delta;
            delta_array_max_ = delta;
        }

        delta_array_min_ = std::min(delta, delta_array_min_);
        delta_array_max_ = std::max(delta, delta_array_max_);
        delta_array_[write_index_++] = delta;

        if (write_index_ >= block_size_) {
            flush_internal();
        }
    }

    int32_t flush(uint8_t*& data) {
        if (write_index_ != -1) {
            flush_internal();
        }
        data = data_array_;
        return cur_position_;
    }

    // Flush data into local data array.
    void flush_internal() {
        uint8_t* data = nullptr;
        int data_size = write_to_binary(data);
        if (data_size == 0) {
            return;
        }
        if (cap_ - cur_position_ < data_size) {
            cap_ *= 2;
            data_array_ = (uint8_t*)realloc(data_array_, cap_);
        }
        memcpy(data_array_ + cur_position_, data, data_size);
        free(data);
        cur_position_ += data_size;
    }

    int write_to_binary(uint8_t*& data) {
        uint8_t* value = nullptr;
        if (write_index_ == -1) {
            return 0;
        }

        for (int i = 0; i < write_index_; ++i) {
            delta_array_[i] -= delta_array_min_;
        }

        int bit_width = 0;
        T delta_max = delta_array_max_ - delta_array_min_;
        while (delta_max > 0) {
            bit_width++;
            delta_max >>= 1;
        }

        int size = 4 * 4 + (bit_width * write_index_ + 7) / 8;
        value = (uint8_t*)malloc(size);
        data = value;

        write_uint32(write_index_, value);
        write_uint32(bit_width, value);
        if constexpr (std::is_same<T, int32_t>::value) {
            write_uint32(static_cast<uint32_t>(delta_array_min_), value);
            write_uint32(static_cast<uint32_t>(first_value_), value);
        } else if constexpr (std::is_same<T, int64_t>::value) {
            write_uint64(static_cast<uint64_t>(delta_array_min_), value);
            write_uint64(static_cast<uint64_t>(first_value_), value);
        } else {
            static_assert(always_false<T>::value, "Unsupport type T");
        }

        for (int i = 0; i < write_index_; i++) {
            write_bits(delta_array_[i], bit_width, value);
        }

        if (bits_left_ != 0 && bits_left_ != 8) {
            bits_left_ = 0;
            flush_byte_if_full(value);
        }
        write_index_ = -1;
        return size;
    }

    void write_bits(int64_t value, int bits, uint8_t*& out_stream) {
        while (bits > 0) {
            int shift = bits - bits_left_;
            if (shift >= 0) {
                buffer_ |=
                    (uint8_t)((value >> shift) & ((1 << bits_left_) - 1));
                bits -= bits_left_;
                bits_left_ = 0;
            } else {
                shift = bits_left_ - bits;
                buffer_ |= (uint8_t)(value << shift);
                bits_left_ -= bits;
                bits = 0;
            }
            flush_byte_if_full(out_stream);
        }
    }

    void flush_byte_if_full(uint8_t*& out_stream) {
        if (bits_left_ == 0) {
            memcpy(out_stream, &buffer_, 1);
            out_stream++;
            buffer_ = 0;
            bits_left_ = 8;
        }
    }

   private:
    int block_size_;
    T* delta_array_;
    T first_value_;
    T previous_value_;
    T delta_array_min_;
    T delta_array_max_;
    uint8_t buffer_;
    uint8_t* data_array_ = nullptr;
    int32_t cap_;
    int32_t cur_position_;
    int bits_left_;
    int write_index_;
};

template <typename T,
          typename =
              typename std::enable_if<std::is_same<T, int32_t>::value ||
                                      std::is_same<T, int64_t>::value>::type>
class ts_2diff_decoder {
   public:
    explicit ts_2diff_decoder(uint8_t* data_array) {
        write_index_ = -1;
        bits_left_ = 0;
        stored_value_ = 0;
        buffer_ = 0;
        delta_min_ = 0;
        previous_value_ = 0;
        bit_width_ = 0;
        current_index_ = 0;
        data_array_ = data_array;
    }

    T decode() {
        T ret_value = 0;
        if (current_index_ == 0) {
            write_index_ = read_ui32(data_array_);
            bit_width_ = read_ui32(data_array_);
            if constexpr (std::is_same<T, int32_t>::value) {
                delta_min_ = read_ui32(data_array_);
                previous_value_ = read_ui32(data_array_);
            } else if constexpr (std::is_same<T, int64_t>::value) {
                delta_min_ = read_ui64(data_array_);
                previous_value_ = read_ui64(data_array_);
            } else {
                static_assert(always_false<T>::value, "Unsupport type T");
            }
            ret_value = previous_value_;
            if (write_index_ == 0) {
                current_index_ = 0;
            } else {
                current_index_ = 1;
            }
            return ret_value;
        }
        if (current_index_++ >= write_index_) {
            current_index_ = 0;
        }
        stored_value_ = read_long(bit_width_, data_array_);
        ret_value = stored_value_ + previous_value_ + delta_min_;
        previous_value_ = ret_value;
        return ret_value;
    }
    void read_byte_if_empty(uint8_t*& in) {
        if (bits_left_ == 0) {
            memcpy(&buffer_, in, 1);
            bits_left_ = 8;
            in += 1;
        }
    }
    int64_t read_long(int bits, uint8_t*& in) {
        int64_t value = 0;
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
            if (bits <= 0 && current_index_ == 0) {
                break;
            }
        }
        return value;
    }

   private:
    T stored_value_;
    T delta_min_;
    T previous_value_;
    uint8_t buffer_;
    int bits_left_;
    int bit_width_;
    int write_index_;
    int current_index_;
    uint8_t* data_array_;
};

#endif  // TS_2DIFF_H
