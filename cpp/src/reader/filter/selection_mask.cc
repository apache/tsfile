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

#include "reader/filter/selection_mask.h"

#include <algorithm>
#include <cassert>

namespace storage {
namespace {
uint32_t count_bits(uint64_t value) {
#if defined(__GNUC__) || defined(__clang__)
    return static_cast<uint32_t>(__builtin_popcountll(value));
#else
    uint32_t count = 0;
    while (value != 0) {
        value &= value - 1;
        ++count;
    }
    return count;
#endif
}

uint32_t trailing_zeros(uint64_t value) {
    assert(value != 0);
#if defined(__GNUC__) || defined(__clang__)
    return static_cast<uint32_t>(__builtin_ctzll(value));
#else
    uint32_t count = 0;
    while ((value & 1U) == 0) {
        value >>= 1U;
        ++count;
    }
    return count;
#endif
}
}  // namespace

SelectionMask::SelectionMask() : row_count_(0) {}

SelectionMask::SelectionMask(uint32_t row_count) { reset(row_count); }

void SelectionMask::reset(uint32_t row_count) {
    row_count_ = row_count;
    words_.assign(word_count(row_count), 0);
}

void SelectionMask::set_all(bool value) {
    const uint64_t fill = value ? ~static_cast<uint64_t>(0) : 0;
    for (auto& word : words_) {
        word = fill;
    }
    clear_unused_bits();
}

void SelectionMask::set_one(uint32_t row) {
    if (row >= row_count_) return;
    words_[row >> 6U] |= (static_cast<uint64_t>(1) << (row & 63U));
}

bool SelectionMask::test(uint32_t row) const {
    if (row >= row_count_) return false;
    return (words_[row >> 6U] & (static_cast<uint64_t>(1) << (row & 63U))) != 0;
}

void SelectionMask::and_with(const SelectionMask& other) {
    assert(row_count_ == other.row_count_);
    const size_t common = std::min(words_.size(), other.words_.size());
    for (size_t i = 0; i < common; ++i) {
        words_[i] &= other.words_[i];
    }
    // Rows past the shorter mask have no counterpart; ANDing with nothing
    // clears them so a mismatched operand cannot leave stale bits.
    for (size_t i = common; i < words_.size(); ++i) {
        words_[i] = 0;
    }
}

void SelectionMask::or_with(const SelectionMask& other) {
    assert(row_count_ == other.row_count_);
    const size_t common = std::min(words_.size(), other.words_.size());
    for (size_t i = 0; i < common; ++i) {
        words_[i] |= other.words_[i];
    }
    // Rows past the shorter mask keep their value (ORing with nothing).
}

void SelectionMask::invert() {
    for (auto& word : words_) {
        word = ~word;
    }
    clear_unused_bits();
}

uint32_t SelectionMask::count_ones() const {
    uint32_t count = 0;
    for (uint64_t word : words_) {
        count += count_bits(word);
    }
    return count;
}

bool SelectionMask::next_set_bit(uint32_t from, uint32_t& next) const {
    if (from >= row_count_) return false;
    uint32_t word_index = from >> 6U;
    uint64_t word =
        words_[word_index] & (~static_cast<uint64_t>(0) << (from & 63U));
    while (true) {
        if (word != 0) {
            const uint32_t candidate =
                (word_index << 6U) + trailing_zeros(word);
            if (candidate >= row_count_) return false;
            next = candidate;
            return true;
        }
        ++word_index;
        if (word_index >= words_.size()) return false;
        word = words_[word_index];
    }
}

bool SelectionMask::next_selected_run(uint32_t from, uint32_t& begin,
                                      uint32_t& end) const {
    if (!next_set_bit(from, begin)) return false;
    uint32_t word_index = begin >> 6U;
    uint64_t unselected =
        ~words_[word_index] & (~static_cast<uint64_t>(0) << (begin & 63U));
    while (true) {
        if (unselected != 0) {
            end = std::min(row_count_,
                           (word_index << 6U) + trailing_zeros(unselected));
            return true;
        }
        if (++word_index >= words_.size()) {
            end = row_count_;
            return true;
        }
        unselected = ~words_[word_index];
    }
}

void SelectionMask::clear_from(uint32_t row) {
    if (row >= row_count_) return;
    const uint32_t first_word = row >> 6U;
    const uint32_t bit = row & 63U;
    words_[first_word] &=
        bit == 0 ? 0 : ((static_cast<uint64_t>(1) << bit) - 1U);
    for (size_t i = first_word + 1; i < words_.size(); ++i) {
        words_[i] = 0;
    }
}

void SelectionMask::clear_unused_bits() {
    if (words_.empty()) return;
    const uint32_t used = row_count_ & 63U;
    if (used != 0) {
        words_.back() &= ((static_cast<uint64_t>(1) << used) - 1U);
    }
}

}  // namespace storage
