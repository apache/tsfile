/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#ifndef READER_FILTER_SELECTION_MASK_H
#define READER_FILTER_SELECTION_MASK_H

#include <cstddef>
#include <cstdint>
#include <vector>

namespace storage {

// A compact row-selection bitmap. Bit i corresponds to row i in an aligned
// batch. All operations require masks with the same row count.
class SelectionMask {
   public:
    SelectionMask();
    explicit SelectionMask(uint32_t row_count);

    void reset(uint32_t row_count);
    void set_all(bool value = true);
    // Releases the backing storage. Readers are torn down through an explicit
    // destroy() that never runs destructors, so clear() alone would leak it.
    void release_storage();

    uint32_t row_count() const { return row_count_; }
    bool empty() const { return count_ones() == 0; }

    void set_one(uint32_t row);
    bool test(uint32_t row) const;

    void and_with(const SelectionMask& other);
    void or_with(const SelectionMask& other);
    void invert();

    uint32_t count_ones() const;
    bool next_set_bit(uint32_t from, uint32_t& next) const;
    // Find the next contiguous selected interval [begin, end), starting at
    // or after from. The end is clamped to row_count(), excluding padding.
    bool next_selected_run(uint32_t from, uint32_t& begin, uint32_t& end) const;
    void clear_from(uint32_t row);

   private:
    static size_t word_count(uint32_t row_count) {
        return (static_cast<uint64_t>(row_count) + 63U) >> 6U;
    }
    void clear_unused_bits();

    std::vector<uint64_t> words_;
    uint32_t row_count_;
};

}  // namespace storage

#endif  // READER_FILTER_SELECTION_MASK_H
