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

 #ifndef ENCODING_ACLUSTER_DECODER_H
#define ENCODING_ACLUSTER_DECODER_H

#include <vector>
#include <string>
#include <memory>
#include <cmath>
#include <stdexcept>
#include <limits>
#include <cstdint>

#include "decoder.h"
#include "common/allocator/byte_stream.h"

// ===================================================================
//   Part 1: Core ACluster Decoding Logic
// ===================================================================
namespace AClusterDecodeLogic {
// The entire logic here is identical to KClusterDecodeLogic, as they share the same decode_multidim_impl
// Corresponds to: cluster_encoder_logic.cpp :: BitStreamReader
class BitStreamReader {
public:
    explicit BitStreamReader(const std::vector<uint8_t>& data)
        : buffer(data), bufferSize(data.size() * 8), currentBitPos(0) {}

    inline long long readBits(int numBits) {
        if (numBits < 0 || numBits > 64) throw std::invalid_argument("Cannot read " + std::to_string(numBits) + " bits.");
        if (currentBitPos + numBits > bufferSize) throw std::out_of_range("Not enough bits in stream to read " + std::to_string(numBits) + " bits.");
        
        long long value = 0;
        for (int i = 0; i < numBits; ++i) {
            size_t byteIndex = currentBitPos / 8;
            int bitIndex = 7 - (currentBitPos % 8);
            bool bit = (buffer[byteIndex] & (1 << bitIndex)) != 0;
            value = (value << 1) | (bit ? 1LL : 0LL);
            currentBitPos++;
        }
        return value;
    }

    inline bool has_more() const {
        return currentBitPos < bufferSize;
    }

private:
    const std::vector<uint8_t>& buffer;
    const size_t bufferSize;
    size_t currentBitPos;
};

// Corresponds to: cluster_encoder_logic.cpp :: zigzagDecode
inline long long zigzagDecode(long long n) { return (n >> 1) ^ (-(n & 1)); }

// Corresponds to: cluster_encoder_logic.cpp :: decode_multidim_impl
inline std::vector<std::vector<double>> decode_multidim_impl(const std::vector<uint8_t>& compressed_data) {
    if (compressed_data.empty()) return {};
    BitStreamReader reader(compressed_data);

    int dim = reader.readBits(8);
    int pack_size = reader.readBits(16);
    int block_size = reader.readBits(16);
    int page_count = reader.readBits(32);
    int page_size = reader.readBits(16);
    (void)pack_size; (void)page_size; // Suppress unused variable warnings

    std::vector<double> pow10_lookup; for (int i = 0; i < 20; ++i) pow10_lookup.push_back(std::pow(10, i));
    std::vector<std::vector<double>> all_data_rows;
    
    for (int i = 0; i < page_count; ++i) {
        int k = reader.readBits(16);
        int page_data_points = reader.readBits(16);

        std::vector<int> max_decimals(dim);
        std::vector<long long> min_values(dim);
        for (int j = 0; j < dim; ++j) {
            max_decimals[j] = reader.readBits(8);
            int bit_len = reader.readBits(8);
            long long sign = reader.readBits(1);
            long long abs_val = reader.readBits(bit_len);
            min_values[j] = (sign == 0) ? abs_val : -abs_val;
        }
        
        std::vector<long long> min_bases(dim);
        std::vector<int> max_base_bit_len(dim);
        for (int j = 0; j < dim; ++j) {
            int bit_len = reader.readBits(8);
            long long sign = reader.readBits(1);
            long long abs_val = reader.readBits(bit_len);
            min_bases[j] = (sign == 0) ? abs_val : -abs_val;
            max_base_bit_len[j] = reader.readBits(8);
        }
        
        int pack_num = reader.readBits(16);
        std::vector<std::vector<int>> pack_metadata(pack_num, std::vector<int>(dim));
        for (int p = 0; p < pack_num; ++p) for (int j = 0; j < dim; ++j) pack_metadata[p][j] = reader.readBits(8);
        
        std::vector<std::vector<long long>> medoids_long(k, std::vector<long long>(dim));
        for (int m = 0; m < k; ++m) for (int j = 0; j < dim; ++j) medoids_long[m][j] = reader.readBits(max_base_bit_len[j]) + min_bases[j];
        
        std::vector<long long> cluster_sizes(k, 0);
        if (k > 0) {
            int num_freq_blocks = (k + block_size - 1) / block_size;
            std::vector<int> max_bits_per_block(num_freq_blocks);
            for (int b = 0; b < num_freq_blocks; ++b) max_bits_per_block[b] = reader.readBits(8);
            std::vector<long long> deltas(k);
            int freq_idx = 0;
            for (int b = 0; b < num_freq_blocks && freq_idx < k; ++b) {
                int max_bit_for_this_block = max_bits_per_block[b];
                int end = std::min((b + 1) * block_size, k);
                for (int j = freq_idx; j < end; ++j) deltas[j] = reader.readBits(max_bit_for_this_block);
                freq_idx = end;
            }
            cluster_sizes[0] = deltas[0];
            for (size_t j = 1; j < deltas.size(); ++j) cluster_sizes[j] = cluster_sizes[j - 1] + deltas[j];
        }
        
        std::vector<std::vector<long long>> residual_series(page_data_points, std::vector<long long>(dim));
        int data_counter = 0;
        if (page_data_points > 0) {
            for (int p = 0; p < pack_num; ++p) {
                const auto& bits = pack_metadata[p];
                int points_in_pack = (p == pack_num - 1) ? (page_data_points - (p * pack_size)) : pack_size;
                for (int pt = 0; pt < points_in_pack; ++pt) {
                    if (data_counter >= page_data_points) break;
                    for (int j = 0; j < dim; ++j) residual_series[data_counter][j] = zigzagDecode(reader.readBits(bits[j]));
                    data_counter++;
                }
            }
        }
        
        if (page_data_points > 0 && k > 0) {
            int current_point_idx = 0;
            for (int medoid_idx = 0; medoid_idx < k; ++medoid_idx) {
                long long points_in_this_cluster = cluster_sizes[medoid_idx];
                const auto& base_point = medoids_long[medoid_idx];
                for (int p_count = 0; p_count < points_in_this_cluster; ++p_count) {
                    if (current_point_idx < page_data_points) {
                        std::vector<double> row(dim);
                        for (int j = 0; j < dim; ++j) {
                            long long int_val = base_point[j] + residual_series[current_point_idx][j] + min_values[j];
                            row[j] = (max_decimals[j] > 0) ? (static_cast<double>(int_val) / pow10_lookup[max_decimals[j]]) : static_cast<double>(int_val);
                        }
                        all_data_rows.push_back(row);
                        current_point_idx++;
                    }
                }
            }
        } else if (page_data_points > 0) {
            for (int d = 0; d < page_data_points; ++d) {
                std::vector<double> row(dim);
                for (int j = 0; j < dim; ++j) {
                     long long int_val = residual_series[d][j] + min_values[j];
                     row[j] = (max_decimals[j] > 0) ? (static_cast<double>(int_val) / pow10_lookup[max_decimals[j]]) : static_cast<double>(int_val);
                }
                all_data_rows.push_back(row);
            }
        }
    }
    return all_data_rows;
}

} // namespace AClusterDecodeLogic


// ===================================================================
//   Part 2: The Decoder classes that adapt the logic for TsFile
// ===================================================================
namespace storage {

template<typename T>
class AClusterDecoderBase : public Decoder {
public:
    AClusterDecoderBase() : current_read_index_(0), decoded_(false) {}
    ~AClusterDecoderBase() override {}

    int read_boolean(bool&, common::ByteStream&) override { return common::E_NOT_SUPPORT; }
    int read_int32(int32_t&, common::ByteStream&) override { return common::E_NOT_SUPPORT; }
    int read_int64(long long&, common::ByteStream&) override { return common::E_NOT_SUPPORT; }
    int read_float(float&, common::ByteStream&) override { return common::E_NOT_SUPPORT; }
    int read_double(double&, common::ByteStream&) override { return common::E_NOT_SUPPORT; }
    int read_String(common::String&, common::PageArena&, common::ByteStream&) override { return common::E_NOT_SUPPORT; }

protected:
    void decode_page_if_needed(common::ByteStream &in);
    std::vector<T> decoded_points_;
    size_t current_read_index_;
    bool decoded_;
};

class DoubleAClusterDecoder : public AClusterDecoderBase<double> {
public:
    int read_double(double& val, common::ByteStream& in) override;
    int read_float(float& val, common::ByteStream& in) override;
};

class IntAClusterDecoder : public AClusterDecoderBase<long long> {
public:
    int read_int64(long long& val, common::ByteStream& in) override;
    int read_int32(int32_t& val, common::ByteStream& in) override;
};


// --- Implementation ---
template<typename T>
inline void AClusterDecoderBase<T>::decode_page_if_needed(common::ByteStream &in) {
    if (decoded_ || !in.has_remaining()) return;

    uint32_t data_size = 0;
    uint32_t read_len = 0;
    in.read_buf(reinterpret_cast<char*>(&data_size), sizeof(data_size), read_len);
    if (read_len != sizeof(data_size)) throw std::runtime_error("Failed to read ACluster data block size");
    if (data_size == 0) {
        decoded_ = true;
        return;
    }

    std::vector<uint8_t> compressed_block(data_size);
    in.read_buf(reinterpret_cast<char*>(compressed_block.data()), data_size, read_len);
    if (read_len != data_size) throw std::runtime_error("Failed to read ACluster data block");

    AClusterDecodeLogic::BitBuffer fake_bytestream;
    int dim = 1, pack_size = 10, block_size = 10, page_count = 1, page_size = 0;
    AClusterDecodeLogic::BitBufferUtils::appendToBitstream(fake_bytestream, dim, 8);
    AClusterDecodeLogic::BitBufferUtils::appendToBitstream(fake_bytestream, pack_size, 16);
    AClusterDecodeLogic::BitBufferUtils::appendToBitstream(fake_bytestream, block_size, 16);
    AClusterDecodeLogic::BitBufferUtils::appendToBitstream(fake_bytestream, page_count, 32);
    AClusterDecodeLogic::BitBufferUtils::appendToBitstream(fake_bytestream, page_size, 16);
    
    std::vector<uint8_t> final_bytes_to_decode = fake_bytestream.toByteArray();
    final_bytes_to_decode.insert(final_bytes_to_decode.end(), compressed_block.begin(), compressed_block.end());
    
    std::vector<std::vector<double>> result_multidim = AClusterDecodeLogic::decode_multidim_impl(final_bytes_to_decode);
    
    if (!result_multidim.empty()) {
        decoded_points_.reserve(result_multidim.size());
        for(const auto& row : result_multidim) {
            if (!row.empty()) {
                decoded_points_.push_back(static_cast<T>(row[0]));
            }
        }
    }
    decoded_ = true;
}

inline int DoubleAClusterDecoder::read_double(double& val, common::ByteStream& in) {
    try { decode_page_if_needed(in); } catch(const std::exception& e) { return common::E_DECODING_ERROR; }
    if (current_read_index_ >= decoded_points_.size()) return common::E_NO_MORE_DATA;
    val = decoded_points_[current_read_index_++];
    return common::E_OK;
}
inline int DoubleAClusterDecoder::read_float(float& val, common::ByteStream& in) {
    double d_val;
    int ret = read_double(d_val, in);
    if (ret == common::E_OK) val = static_cast<float>(d_val);
    return ret;
}

inline int IntAClusterDecoder::read_int64(long long& val, common::ByteStream& in) {
    try { decode_page_if_needed(in); } catch(const std::exception& e) { return common::E_DECODING_ERROR; }
    if (current_read_index_ >= decoded_points_.size()) return common::E_NO_MORE_DATA;
    val = decoded_points_[current_read_index_++];
    return common::E_OK;
}
inline int IntAClusterDecoder::read_int32(int32_t& val, common::ByteStream& in) {
    long long ll_val;
    int ret = read_int64(ll_val, in);
    if (ret == common::E_OK) {
        if (ll_val < std::numeric_limits<int32_t>::min() || ll_val > std::numeric_limits<int32_t>::max()) {
            return common::E_DECODING_ERROR;
        }
        val = static_cast<int32_t>(ll_val);
    }
    return ret;
}

} // namespace storage
#endif // ENCODING_ACLUSTER_DECODER_H