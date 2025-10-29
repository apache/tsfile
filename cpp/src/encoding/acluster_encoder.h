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

#ifndef ENCODING_ACLUSTER_ENCODER_H
#define ENCODING_ACLUSTER_ENCODER_H

#include <vector>
#include <string>
#include <memory>
#include <cmath>
#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <limits>
#include <unordered_set>
#include <random>
#include <sstream>
#include <functional>
#include <cstdint> // For uint8_t, size_t, etc.

#include "encoder.h"
#include "common/allocator/byte_stream.h"
#include "common/global.h"

// ===================================================================
//   Part 1: Core ACluster Algorithm Logic
// ===================================================================

namespace AClusterLogic {

// Corresponds to: algorithms/cluster_common.h
template<typename T>
struct SoAData {
    std::vector<std::vector<T>> columns;
    int dim = 0;
    int num_rows = 0;
};

// Corresponds to: algorithms/cluster_common.h
struct ClusteringResult {
    std::vector<std::vector<long long>> medoids;
    std::vector<int> cluster_assignment;
    std::vector<long long> cluster_sizes;
};

// Corresponds to: algorithms/cluster_common.h
struct PreprocessorResult {
    std::unique_ptr<SoAData<long long>> data;
    std::vector<int> max_decimal_places;
    std::vector<long long> min_values;
};

// Corresponds to: cluster_encoder_logic.cpp
struct BitBuffer {
    std::vector<uint8_t> buffer;
    size_t currentBitPos = 0;
    
    inline void appendBit(bool bit) {
        size_t byteIndex = currentBitPos / 8;
        int bitIndexInByte = currentBitPos % 8;
        if (byteIndex >= buffer.size()) buffer.push_back(0);
        if (bit) buffer[byteIndex] |= (1 << (7 - bitIndexInByte));
        currentBitPos++;
    }

    inline void merge(const BitBuffer& other) {
        for (size_t i = 0; i < other.currentBitPos; ++i) {
            size_t byteIndex = i / 8;
            int bitOffset = 7 - (i % 8);
            bool bit = (other.buffer[byteIndex] & (1 << bitOffset)) != 0;
            appendBit(bit);
        }
    }
    
    inline std::vector<uint8_t> toByteArray() const { return buffer; }
    inline size_t size() const { return currentBitPos; }
};

namespace BitBufferUtils {
    // Corresponds to: cluster_encoder_logic.cpp :: BitBufferUtils
    inline void appendToBitstream(BitBuffer& bitBuffer, long long num, int bits) {
        for (int i = bits - 1; i >= 0; --i) bitBuffer.appendBit((num >> i) & 1);
    }
    // Corresponds to: cluster_encoder_logic.cpp :: BitBufferUtils
    inline int bitsRequiredNoSign(long long value) {
        if (value == 0) return 1;
        unsigned long long u_value = (value > 0) ? value : -value;
        #if defined(__GNUC__) || defined(__clang__)
        return 64 - __builtin_clzll(u_value);
        #else
        int length = 0; while(u_value > 0){ u_value >>= 1; length++; } return length == 0 ? 1 : length;
        #endif
    }
}

namespace { // Anonymous namespace for internal linkage helpers
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for Preprocessor
    inline int bitLength(long long value) {
        if (value == 0) return 1;
        unsigned long long u_value = (value > 0) ? value : -value;
        #if defined(__GNUC__) || defined(__clang__)
        return 64 - __builtin_clzll(u_value);
        #else
        int length = 0; while (u_value > 0) { u_value >>= 1; length++; } return length;
        #endif
    }
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for clustering
    inline long long calculateTotalLogCost(const std::vector<long long>& p1, const std::vector<long long>& p2, int dim) {
        long long totalCost = 0;
        for (int i = 0; i < dim; ++i) {
            long long residual = p1[i] - p2[i];
            totalCost += bitLength(residual >= 0 ? residual : -residual) + 1;
        }
        return totalCost;
    }
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for clustering
    inline long long calculateBasePointStorageCost(const std::vector<long long>& basePoint, int dim) {
        long long storageCost = 0;
        for (int i = 0; i < dim; ++i) {
            storageCost += bitLength(basePoint[i] >= 0 ? basePoint[i] : -basePoint[i]) + 1;
        }
        return storageCost;
    }
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for clustering
    inline std::vector<long long> get_point_from_soa(const SoAData<long long>& soa_data, int row_idx) {
        std::vector<long long> point(soa_data.dim);
        for (int d = 0; d < soa_data.dim; ++d) {
            point[d] = soa_data.columns[d][row_idx];
        }
        return point;
    }
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for clustering
    inline std::vector<std::vector<long long>> convert_SoA_to_AoS(const SoAData<long long>& soa_data) {
        if (soa_data.num_rows == 0) return {};
        std::vector<std::vector<long long>> aos_data(soa_data.num_rows, std::vector<long long>(soa_data.dim));
        for (int j = 0; j < soa_data.dim; ++j) {
            for (int i = 0; i < soa_data.num_rows; ++i) {
                aos_data[i][j] = soa_data.columns[j][i];
            }
        }
        return aos_data;
    }
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for clustering
    struct VectorHasher {
        inline std::size_t operator()(const std::vector<long long>& vec) const {
            std::size_t seed = vec.size();
            for(long long i : vec) { seed ^= std::hash<long long>()(i) + 0x9e3779b9 + (seed << 6) + (seed >> 2); }
            return seed;
        }
    };
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for clustering
    struct MedSortHelper {
        std::vector<long long> medoid;
        long long size;
        int original_index;
        inline bool operator<(const MedSortHelper& other) const { return size < other.size; }
    };

    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for clustering
    inline ClusteringResult sortResults(std::vector<std::vector<long long>>& medoids, std::vector<int>& assignment, std::vector<long long>& sizes) {
        int k = medoids.size();
        if (k == 0) return {{}, {}, {}};
        std::vector<MedSortHelper> sorters;
        sorters.reserve(k);
        for(int i = 0; i < k; ++i) { sorters.push_back({medoids[i], sizes[i], i}); }
        std::sort(sorters.begin(), sorters.end());
        ClusteringResult res;
        res.medoids.resize(k);
        res.cluster_sizes.resize(k);
        std::vector<int> old_to_new_map(k);
        for(int i = 0; i < k; ++i) {
            res.medoids[i] = sorters[i].medoid;
            res.cluster_sizes[i] = sorters[i].size;
            old_to_new_map[sorters[i].original_index] = i;
        }
        res.cluster_assignment.resize(assignment.size());
        for(size_t i = 0; i < assignment.size(); ++i) {
            if (assignment[i] != -1 && static_cast<size_t>(assignment[i]) < old_to_new_map.size()) {
                res.cluster_assignment[i] = old_to_new_map[assignment[i]];
            } else { res.cluster_assignment[i] = -1; }
        }
        return res;
    }
    // Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for encoding
    inline long long zigzagEncode_scalar(long long n) {
        return (n << 1) ^ (n >> 63);
    }
} // end anonymous namespace

// Corresponds to: cluster_encoder_logic.cpp :: adaptiveGreedyBasePointSelection
inline ClusteringResult adaptiveGreedyBasePointSelection(const SoAData<long long>& page_data, int dim) {
    auto data_aos = convert_SoA_to_AoS(page_data);
    size_t n = data_aos.size();
    if (n == 0) return {{}, {}, {}};
    std::vector<std::vector<long long>> medoids_list;
    std::unordered_set<std::vector<long long>, VectorHasher> existing_medoids_set;
    std::vector<std::unordered_set<int>> points_in_cluster_list;
    std::vector<int> point_to_leader_map(n, -1);
    medoids_list.push_back(data_aos[0]);
    existing_medoids_set.insert(data_aos[0]);
    points_in_cluster_list.emplace_back();
    points_in_cluster_list[0].insert(0);
    point_to_leader_map[0] = 0;
    for (size_t i = 1; i < n; ++i) {
        const auto& current_point = data_aos[i];
        if (existing_medoids_set.count(current_point)) {
            int existing_leader_index = -1;
            for (size_t j = 0; j < medoids_list.size(); ++j) { if (medoids_list[j] == current_point) { existing_leader_index = j; break; } }
            if (existing_leader_index != -1) { points_in_cluster_list[existing_leader_index].insert(i); point_to_leader_map[i] = existing_leader_index; }
            continue;
        }
        int best_leader_index = -1;
        long long min_cost_to_existing_leader = std::numeric_limits<long long>::max();
        for (size_t j = 0; j < medoids_list.size(); ++j) {
            long long cost = calculateTotalLogCost(current_point, medoids_list[j], dim);
            if (cost < min_cost_to_existing_leader) { min_cost_to_existing_leader = cost; best_leader_index = j; }
        }
        long long savings_from_current_point = min_cost_to_existing_leader;
        long long savings_from_reassignment = 0;
        const auto& points_in_best_leader_cluster = points_in_cluster_list[best_leader_index];
        for (int point_index_in_cluster : points_in_best_leader_cluster) {
            const auto& p = data_aos[point_index_in_cluster];
            long long cost_to_old_leader = calculateTotalLogCost(p, medoids_list[best_leader_index], dim);
            long long cost_to_new_potential_leader = calculateTotalLogCost(p, current_point, dim);
            if (cost_to_new_potential_leader < cost_to_old_leader) { savings_from_reassignment += (cost_to_old_leader - cost_to_new_potential_leader); }
        }
        long long total_savings = savings_from_current_point + savings_from_reassignment;
        long long storage_cost_for_new_point = calculateBasePointStorageCost(current_point, dim);
        if (total_savings > storage_cost_for_new_point) {
            int new_leader_id = medoids_list.size();
            medoids_list.push_back(current_point);
            existing_medoids_set.insert(current_point);
            points_in_cluster_list.emplace_back();
            points_in_cluster_list.back().insert(i);
            point_to_leader_map[i] = new_leader_id;
            std::vector<int> points_to_reassign;
            for (int point_idx : points_in_cluster_list[best_leader_index]) {
                const auto& p = data_aos[point_idx];
                long long cost_to_old = calculateTotalLogCost(p, medoids_list[best_leader_index], dim);
                long long cost_to_new = calculateTotalLogCost(p, current_point, dim);
                if (cost_to_new < cost_to_old) { points_to_reassign.push_back(point_idx); }
            }
            for (int point_idx : points_to_reassign) {
                points_in_cluster_list[best_leader_index].erase(point_idx);
                points_in_cluster_list[new_leader_id].insert(point_idx);
                point_to_leader_map[point_idx] = new_leader_id;
            }
        } else {
            points_in_cluster_list[best_leader_index].insert(i);
            point_to_leader_map[i] = best_leader_index;
        }
    }
    int k = medoids_list.size();
    auto discovered_medoids = medoids_list;
    std::vector<long long> raw_cluster_sizes(k);
    for (int i = 0; i < k; ++i) { raw_cluster_sizes[i] = points_in_cluster_list[i].size(); }
    return sortResults(discovered_medoids, point_to_leader_map, raw_cluster_sizes);
}

// Corresponds to: cluster_encoder_logic.cpp :: residualCalculationZigzag_sorted
inline std::vector<std::vector<long long>> residualCalculationZigzag_sorted(
    const SoAData<long long>& data, const std::vector<std::vector<long long>>& medoids,
    const std::vector<int>& assignments, const std::vector<long long>& sorted_cluster_sizes, int dim) 
{
    int n = data.num_rows;
    size_t k = medoids.size();
    if (n == 0) return {};
    std::vector<int> writePointers(k);
    int cumulativeCount = 0;
    for (size_t i = 0; i < k; ++i) {
        writePointers[i] = cumulativeCount;
        cumulativeCount += static_cast<int>(sorted_cluster_sizes[i]);
    }
    std::vector<std::vector<long long>> sortedResidualSeries(n, std::vector<long long>(dim));
    for (int i = 0; i < n; i++) {
        int clusterId = assignments[i];
        if (clusterId < 0 || static_cast<size_t>(clusterId) >= k) continue;
        const auto& base = medoids[clusterId];
        int targetIndex = writePointers[clusterId];
        for (int j = 0; j < dim; j++) {
            long long residual = data.columns[j][i] - base[j];
            sortedResidualSeries[targetIndex][j] = zigzagEncode_scalar(residual);
        }
        writePointers[clusterId]++;
    }
    return sortedResidualSeries;
}

// Corresponds to: cluster_encoder_logic.cpp :: anonymous helpers for encoding
namespace {
    struct BasePointsResult { std::vector<long long> min_base; std::vector<int> max_base_bit_len; BitBuffer base_bitstream; };
    inline BasePointsResult generateBitstreamEncodedBasePointsOptimized(const std::vector<std::vector<long long>>& medoids, int dim) {
        if (medoids.empty()) return {std::vector<long long>(dim, 0), std::vector<int>(dim, 0), BitBuffer()};
        size_t k = medoids.size();
        std::vector<long long> min_base(dim, std::numeric_limits<long long>::max());
        for (const auto& point : medoids) for (int i = 0; i < dim; ++i) min_base[i] = std::min(min_base[i], point[i]);
        std::vector<std::vector<long long>> offset_medoids(k, std::vector<long long>(dim));
        std::vector<int> max_base_bit_len(dim, 0);
        for (size_t i = 0; i < k; ++i) {
            for (int j = 0; j < dim; ++j) {
                long long offset_value = medoids[i][j] - min_base[j];
                offset_medoids[i][j] = offset_value;
                max_base_bit_len[j] = std::max(max_base_bit_len[j], BitBufferUtils::bitsRequiredNoSign(offset_value));
            }
        }
        BitBuffer base_bitstream;
        for (const auto& offset_point : offset_medoids) for (int i = 0; i < dim; ++i) BitBufferUtils::appendToBitstream(base_bitstream, offset_point[i], max_base_bit_len[i]);
        return {min_base, max_base_bit_len, base_bitstream};
    }
    inline BitBuffer generateBitstreamFrequency(const std::vector<long long>& cluster_size, int block_size) {
        if (cluster_size.empty()) return BitBuffer();
        BitBuffer freq_bit, freq_meta;
        std::vector<long long> cluster_delta(cluster_size.size());
        if (!cluster_size.empty()) {
            cluster_delta[0] = cluster_size[0];
            for (size_t i = 1; i < cluster_size.size(); ++i) cluster_delta[i] = cluster_size[i] - cluster_size[i - 1];
        }
        int total_length = cluster_size.size();
        int num_blocks = (total_length + block_size - 1) / block_size;
        for (int block_idx = 0; block_idx < num_blocks; ++block_idx) {
            int start = block_idx * block_size;
            int end = std::min(start + block_size, total_length);
            long long max_frequency = 0;
            for (int i = start; i < end; ++i) max_frequency = std::max(max_frequency, cluster_delta[i]);
            int max_freq_bit = BitBufferUtils::bitsRequiredNoSign(max_frequency);
            BitBufferUtils::appendToBitstream(freq_meta, max_freq_bit, 8);
            for (int i = start; i < end; ++i) BitBufferUtils::appendToBitstream(freq_bit, cluster_delta[i], max_freq_bit);
        }
        freq_meta.merge(freq_bit);
        return freq_meta;
    }
    struct EncodedDataResult { BitBuffer residual_bitstream; std::vector<std::vector<int>> pack_res_metadata; int pack_num; int total_data_points; };
    inline EncodedDataResult generateBitstreamEncodedData(const std::vector<std::vector<long long>>& residual_series, int pack_size, int dim) {
        int total_data_points = residual_series.size();
        if (total_data_points == 0) return {BitBuffer(), {}, 0, 0};
        int pack_num = (total_data_points + pack_size - 1) / pack_size;
        BitBuffer residual_bit;
        std::vector<std::vector<int>> pack_res_metadata(pack_num, std::vector<int>(dim));
        int pack_index = 0;
        for (int pack_start = 0; pack_start < total_data_points; pack_start += pack_size) {
            int pack_end = std::min(pack_start + pack_size, total_data_points);
            std::vector<int> max_residual_bits(dim, 0);
            for (int i = pack_start; i < pack_end; ++i) for (int j = 0; j < dim; ++j) max_residual_bits[j] = std::max(max_residual_bits[j], BitBufferUtils::bitsRequiredNoSign(residual_series[i][j]));
            pack_res_metadata[pack_index] = max_residual_bits;
            for (int i = pack_start; i < pack_end; ++i) for (int j = 0; j < dim; ++j) BitBufferUtils::appendToBitstream(residual_bit, residual_series[i][j], max_residual_bits[j]);
            pack_index++;
        }
        return {residual_bit, pack_res_metadata, pack_num, total_data_points};
    }
} // end anonymous namespace

inline std::unique_ptr<PreprocessorResult> preprocessPageFromDoubles(const std::vector<double>& page_doubles, int& dim) {
    dim = 1;
    if (page_doubles.empty()) return nullptr;
    int max_decimal_places = 0;
    for(double val : page_doubles) {
        std::string s = std::to_string(val);
        auto dot_pos = s.find('.');
        if (dot_pos != std::string::npos) {
            s.erase(s.find_last_not_of('0') + 1, std::string::npos);
            if (!s.empty() && s.back() == '.') s.pop_back();
            max_decimal_places = std::max(max_decimal_places, (int)(s.length() - dot_pos - 1));
        }
    }
    double scale = std::pow(10, max_decimal_places);
    auto integerSoA = std::make_unique<SoAData<long long>>();
    integerSoA->dim = 1;
    integerSoA->num_rows = page_doubles.size();
    integerSoA->columns.resize(1);
    integerSoA->columns[0].reserve(page_doubles.size());
    for(double val : page_doubles) {
        integerSoA->columns[0].push_back(static_cast<long long>(val * scale));
    }
    long long min_val_long = 0;
    if(!integerSoA->columns[0].empty()){
        min_val_long = *std::min_element(integerSoA->columns[0].begin(), integerSoA->columns[0].end());
    }
    for (long long& val : integerSoA->columns[0]) {
        val -= min_val_long;
    }
    return std::make_unique<PreprocessorResult>(PreprocessorResult{std::move(integerSoA), {max_decimal_places}, {min_val_long}});
}

} // namespace AClusterLogic

namespace storage {

// Base class for holding common data for ACluster encoders.
template<typename BufferType>
class AClusterEncoderData {
protected:
    AClusterEncoderData() {
        page_size_ = common::g_config_value_.page_writer_max_point_num_;
        points_buffer_.reserve(page_size_);
    }

    void reset_data() {
        points_buffer_.clear();
    }

    std::vector<BufferType> points_buffer_;
    uint32_t page_size_;
};

// ====================================================================
// DoubleAClusterEncoder for FLOAT and DOUBLE
// ====================================================================
class DoubleAClusterEncoder : public Encoder, private AClusterEncoderData<double> {
public:
    DoubleAClusterEncoder() = default;
    ~DoubleAClusterEncoder() override {}

    void reset() override { reset_data(); }

    int get_max_byte_size() override {
        return page_size_ * sizeof(double) * 2;
    }

    // --- Overrides for supported types ---
    int encode(double value, common::ByteStream &out_stream) override {
        points_buffer_.push_back(value);
        return common::E_OK;
    }
    int encode(float value, common::ByteStream &out_stream) override {
        points_buffer_.push_back(static_cast<double>(value));
        return common::E_OK;
    }

    // --- Override for flush ---
    int flush(common::ByteStream &out_stream) override;

    // --- Overrides for unsupported types ---
    int encode(bool, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(int32_t, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(int64_t, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(common::String, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
};

// ====================================================================
// IntAClusterEncoder for INT32 and INT64
// ====================================================================
class IntAClusterEncoder : public Encoder, private AClusterEncoderData<long long> {
public:
    IntAClusterEncoder() = default;
    ~IntAClusterEncoder() override {}

    void reset() override { reset_data(); }

    int get_max_byte_size() override {
        return page_size_ * sizeof(long long) * 2;
    }

    // --- Overrides for supported types ---
    int encode(long long value, common::ByteStream &out_stream) override {
        points_buffer_.push_back(value);
        return common::E_OK;
    }
    int encode(int32_t value, common::ByteStream &out_stream) override {
        points_buffer_.push_back(static_cast<long long>(value));
        return common::E_OK;
    }

    // --- Override for flush ---
    int flush(common::ByteStream &out_stream) override;

    // --- Overrides for unsupported types ---
    int encode(bool, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(float, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(double, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
    int encode(common::String, common::ByteStream &) override { return common::E_TYPE_NOT_MATCH; }
};


// ====================================================================
// Implementation of flush methods
// ====================================================================

// --- FLUSH IMPLEMENTATION FOR DoubleAClusterEncoder ---
inline int DoubleAClusterEncoder::flush(common::ByteStream &out_stream) {
    if (points_buffer_.empty()) {
        return common::E_OK;
    }
    try {
        int dim = 1;
        int pack_size = 10;
        int block_size = 10;

        auto preproc_res_ptr = AClusterLogic::preprocessPageFromDoubles(points_buffer_, dim);
        if (!preproc_res_ptr || !preproc_res_ptr->data) {
            reset();
            return common::E_OK; 
        }
        
        // <<<<<<<<<<<< CORE LOGIC CHANGE >>>>>>>>>>>>
        // Call ACluster's core clustering algorithm
        AClusterLogic::ClusteringResult clustering_res = AClusterLogic::adaptiveGreedyBasePointSelection(*preproc_res_ptr->data, dim);
        
        using namespace AClusterLogic;
        int page_k = clustering_res.medoids.size();
        BitBuffer fre_bitstream = generateBitstreamFrequency(clustering_res.cluster_sizes, block_size);
        std::vector<std::vector<long long>> residuals = residualCalculationZigzag_sorted(*preproc_res_ptr->data, clustering_res.medoids, clustering_res.cluster_assignment, clustering_res.cluster_sizes, dim);
        
        EncodedDataResult bitstream_res = generateBitstreamEncodedData(residuals, pack_size, dim);
        BasePointsResult basepoint_res = generateBitstreamEncodedBasePointsOptimized(clustering_res.medoids, dim);

        BitBuffer single_page_bitstream;
        BitBufferUtils::appendToBitstream(single_page_bitstream, (long long)page_k, 16);
        BitBufferUtils::appendToBitstream(single_page_bitstream, bitstream_res.total_data_points, 16);
        for (int j = 0; j < dim; ++j) {
            BitBufferUtils::appendToBitstream(single_page_bitstream, preproc_res_ptr->max_decimal_places[j], 8);
            long long val = preproc_res_ptr->min_values[j]; long long abs_val = std::abs(val); int bit_len = BitBufferUtils::bitsRequiredNoSign(abs_val);
            BitBufferUtils::appendToBitstream(single_page_bitstream, bit_len, 8); BitBufferUtils::appendToBitstream(single_page_bitstream, (val >= 0 ? 0 : 1), 1); BitBufferUtils::appendToBitstream(single_page_bitstream, abs_val, bit_len);
        }
        for (int j = 0; j < dim; ++j) {
            long long val = basepoint_res.min_base[j]; long long abs_val = std::abs(val); int bit_len = BitBufferUtils::bitsRequiredNoSign(abs_val);
            BitBufferUtils::appendToBitstream(single_page_bitstream, bit_len, 8); BitBufferUtils::appendToBitstream(single_page_bitstream, (val >= 0 ? 0 : 1), 1); BitBufferUtils::appendToBitstream(single_page_bitstream, abs_val, bit_len);
            BitBufferUtils::appendToBitstream(single_page_bitstream, basepoint_res.max_base_bit_len[j], 8);
        }
        BitBufferUtils::appendToBitstream(single_page_bitstream, (long long)bitstream_res.pack_res_metadata.size(), 16);
        for (const auto& pack : bitstream_res.pack_res_metadata) for (int val : pack) BitBufferUtils::appendToBitstream(single_page_bitstream, val, 8);
        single_page_bitstream.merge(basepoint_res.base_bitstream);
        single_page_bitstream.merge(fre_bitstream);
        single_page_bitstream.merge(bitstream_res.residual_bitstream);
        
        std::vector<uint8_t> compressed_bytes = single_page_bitstream.toByteArray();
        uint32_t data_size = compressed_bytes.size();
        out_stream.write_buf(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
        if (data_size > 0) {
            out_stream.write_buf(reinterpret_cast<const char*>(compressed_bytes.data()), data_size);
        }
    } catch (const std::exception& e) {
        reset();
        return common::E_ENCODING_ERROR;
    }
    reset();
    return common::E_OK;
}

// --- FLUSH IMPLEMENTATION FOR IntAClusterEncoder ---
inline int IntAClusterEncoder::flush(common::ByteStream &out_stream) {
    if (points_buffer_.empty()) {
        return common::E_OK;
    }
    try {
        int dim = 1;
        int pack_size = 10;
        int block_size = 10;
        
        auto preproc_res_ptr = AClusterLogic::preprocessPageFromLongs(points_buffer_, dim);
        if (!preproc_res_ptr || !preproc_res_ptr->data) {
            reset();
            return common::E_OK; 
        }
        
        // <<<<<<<<<<<< CORE LOGIC CHANGE >>>>>>>>>>>>
        // Call ACluster's core clustering algorithm
        AClusterLogic::ClusteringResult clustering_res = AClusterLogic::adaptiveGreedyBasePointSelection(*preproc_res_ptr->data, dim);
        
        using namespace AClusterLogic;
        int page_k = clustering_res.medoids.size();
        BitBuffer fre_bitstream = generateBitstreamFrequency(clustering_res.cluster_sizes, block_size);
        std::vector<std::vector<long long>> residuals = residualCalculationZigzag_sorted(*preproc_res_ptr->data, clustering_res.medoids, clustering_res.cluster_assignment, clustering_res.cluster_sizes, dim);
        
        EncodedDataResult bitstream_res = generateBitstreamEncodedData(residuals, pack_size, dim);
        BasePointsResult basepoint_res = generateBitstreamEncodedBasePointsOptimized(clustering_res.medoids, dim);

        BitBuffer single_page_bitstream;
        BitBufferUtils::appendToBitstream(single_page_bitstream, (long long)page_k, 16);
        BitBufferUtils::appendToBitstream(single_page_bitstream, bitstream_res.total_data_points, 16);
        for (int j = 0; j < dim; ++j) {
            BitBufferUtils::appendToBitstream(single_page_bitstream, preproc_res_ptr->max_decimal_places[j], 8);
            long long val = preproc_res_ptr->min_values[j]; long long abs_val = std::abs(val); int bit_len = BitBufferUtils::bitsRequiredNoSign(abs_val);
            BitBufferUtils::appendToBitstream(single_page_bitstream, bit_len, 8); BitBufferUtils::appendToBitstream(single_page_bitstream, (val >= 0 ? 0 : 1), 1); BitBufferUtils::appendToBitstream(single_page_bitstream, abs_val, bit_len);
        }
        for (int j = 0; j < dim; ++j) {
            long long val = basepoint_res.min_base[j]; long long abs_val = std::abs(val); int bit_len = BitBufferUtils::bitsRequiredNoSign(abs_val);
            BitBufferUtils::appendToBitstream(single_page_bitstream, bit_len, 8); BitBufferUtils::appendToBitstream(single_page_bitstream, (val >= 0 ? 0 : 1), 1); BitBufferUtils::appendToBitstream(single_page_bitstream, abs_val, bit_len);
            BitBufferUtils::appendToBitstream(single_page_bitstream, basepoint_res.max_base_bit_len[j], 8);
        }
        BitBufferUtils::appendToBitstream(single_page_bitstream, (long long)bitstream_res.pack_res_metadata.size(), 16);
        for (const auto& pack : bitstream_res.pack_res_metadata) for (int val : pack) BitBufferUtils::appendToBitstream(single_page_bitstream, val, 8);
        single_page_bitstream.merge(basepoint_res.base_bitstream);
        single_page_bitstream.merge(fre_bitstream);
        single_page_bitstream.merge(bitstream_res.residual_bitstream);
        
        std::vector<uint8_t> compressed_bytes = single_page_bitstream.toByteArray();
        uint32_t data_size = compressed_bytes.size();
        out_stream.write_buf(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
        if (data_size > 0) {
            out_stream.write_buf(reinterpret_cast<const char*>(compressed_bytes.data()), data_size);
        }
    } catch (const std::exception& e) {
        reset();
        return common::E_ENCODING_ERROR;
    }
    reset();
    return common::E_OK;
}

} // namespace storage

#endif // ENCODING_ACLUSTER_ENCODER_H