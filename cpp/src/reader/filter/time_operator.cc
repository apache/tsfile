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
#include "time_operator.h"

#include <cstring>

#include "common/statistic.h"
#include "utils/storage_utils.h"

#if defined(__ARM_NEON)
#include <arm_neon.h>
#elif defined(ENABLE_SIMD)
#include "simde/x86/avx2.h"
#endif

namespace storage {

TimeBetween::TimeBetween(int64_t value1, int64_t value2, bool not_between)
    : value1_(value1), value2_(value2), not_(not_between), type_(TIME_FILTER) {}

TimeBetween::~TimeBetween() {}

bool TimeBetween::satisfy(Statistic* statistic) {
    if (not_) {
        return statistic->end_time_ < value1_ ||
               statistic->start_time_ > value2_;
    } else {
        return statistic->end_time_ >= value1_ &&
               statistic->start_time_ <= value2_;
    }
}

bool TimeBetween::satisfy(int64_t time, int64_t value) {
    return (value1_ <= time) && (time <= value2_) ^ not_;
}

bool TimeBetween::satisfy(int64_t time, common::String value) {
    return (value1_ <= time) && (time <= value2_) ^ not_;
}

bool TimeBetween::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    if (not_) {
        return start_time < value1_ || end_time > value2_;
    } else {
        return end_time >= value1_ && start_time <= value2_;
    }
}

bool TimeBetween::contain_start_end_time(int64_t start_time, int64_t end_time) {
    if (not_) {
        return end_time < value1_ || start_time > value2_;
    } else {
        return start_time >= value1_ && end_time <= value2_;
    }
}

std::vector<TimeRange*>* TimeBetween::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    if (not_) {
        if (value1_ != std::numeric_limits<int64_t>::min()) {
            result->push_back(new TimeRange(std::numeric_limits<int64_t>::min(),
                                            value1_ - 1));
        }
        if (value2_ != std::numeric_limits<int64_t>::max()) {
            result->push_back(new TimeRange(
                value2_ + 1, std::numeric_limits<int64_t>::max()));
        }
    } else {
        result->push_back(new TimeRange(value1_, value2_));
    }
    return result;
}

// TimeIn
TimeIn::TimeIn(const std::vector<int64_t>& values, bool not_in)
    : values_(values), type_(TIME_FILTER), not_(not_in) {}

TimeIn::~TimeIn() {}

bool TimeIn::satisfy(Statistic* statistic) { return true; }

bool TimeIn::satisfy(int64_t time, int64_t value) {
    std::vector<int64_t>::iterator it =
        find(values_.begin(), values_.end(), time);
    bool result = (it != values_.end() ? true : false);
    return result != not_;
}

bool TimeIn::satisfy(int64_t time, common::String value) {
    std::vector<int64_t>::iterator it =
        find(values_.begin(), values_.end(), time);
    bool result = (it != values_.end() ? true : false);
    return result != not_;
}

bool TimeIn::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    return true;
}

bool TimeIn::contain_start_end_time(int64_t start_time, int64_t end_time) {
    return true;
}

std::vector<TimeRange*>* TimeIn::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    int size = values_.size();
    for (int i = 0; i < size; ++i) {
        result->push_back(new TimeRange(values_[i], values_[i]));
    }
    return result;
}

// TimeEq
TimeEq::TimeEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeEq::~TimeEq() {}

bool TimeEq::satisfy(Statistic* statistic) {
    return value_ >= statistic->start_time_ && value_ <= statistic->end_time_;
}

bool TimeEq::satisfy(int64_t time, int64_t value) { return value_ == time; }

bool TimeEq::satisfy(int64_t time, common::String value) {
    return value_ == time;
}

bool TimeEq::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ <= end_time && value_ >= start_time;
}

bool TimeEq::contain_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ == start_time && value_ == end_time;
}

std::vector<TimeRange*>* TimeEq::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    result->push_back(new TimeRange(value_, value_));
    return result;
}

// TimeNotEq
TimeNotEq::TimeNotEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeNotEq::~TimeNotEq() {}

bool TimeNotEq::satisfy(Statistic* statistic) {
    return !(value_ == statistic->start_time_ &&
             value_ == statistic->end_time_);
}

bool TimeNotEq::satisfy(int64_t time, int64_t value) {
    return !(value_ == time);
}

bool TimeNotEq::satisfy(int64_t time, common::String value) {
    return !(value_ == time);
}

bool TimeNotEq::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ != end_time && value_ != start_time;
}

bool TimeNotEq::contain_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ < start_time || value_ > end_time;
}

std::vector<TimeRange*>* TimeNotEq::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    if (value_ == std::numeric_limits<int64_t>::min()) {
        result->push_back(
            new TimeRange(value_ + 1, std::numeric_limits<int64_t>::max()));
    } else if (value_ == std::numeric_limits<int64_t>::max()) {
        result->push_back(
            new TimeRange(std::numeric_limits<int64_t>::min(), value_ - 1));
    } else {
        result->push_back(
            new TimeRange(std::numeric_limits<int64_t>::min(), value_ - 1));
        result->push_back(
            new TimeRange(value_ + 1, std::numeric_limits<int64_t>::max()));
    }
    return result;
}

// TimeGt
TimeGt::TimeGt(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeGt::~TimeGt() {}

bool TimeGt::satisfy(Statistic* statistic) {
    return value_ < statistic->end_time_;
}

bool TimeGt::satisfy(int64_t time, int64_t value) { return value_ < time; }

bool TimeGt::satisfy(int64_t time, common::String value) {
    return value_ < time;
}

bool TimeGt::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ < end_time;
}

bool TimeGt::contain_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ < start_time;
}

std::vector<TimeRange*>* TimeGt::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    if (value_ != std::numeric_limits<int64_t>::max()) {
        result->push_back(
            new TimeRange(value_ + 1, std::numeric_limits<int64_t>::max()));
    }
    return result;
}

// TimeGtEq
TimeGtEq::TimeGtEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeGtEq::~TimeGtEq() {}

bool TimeGtEq::satisfy(Statistic* statistic) {
    return value_ <= statistic->end_time_;
}

bool TimeGtEq::satisfy(int64_t time, int64_t value) { return value_ <= time; }

bool TimeGtEq::satisfy(int64_t time, common::String value) {
    return value_ <= time;
}

bool TimeGtEq::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ <= end_time;
}

bool TimeGtEq::contain_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ <= start_time;
}

std::vector<TimeRange*>* TimeGtEq::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    result->push_back(
        new TimeRange(value_, std::numeric_limits<int64_t>::max()));
    return result;
}

// TimeLt
TimeLt::TimeLt(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeLt::~TimeLt() {}

bool TimeLt::satisfy(Statistic* statistic) {
    return value_ > statistic->start_time_;
}

bool TimeLt::satisfy(int64_t time, int64_t value) { return value_ > time; }

bool TimeLt::satisfy(int64_t time, common::String value) {
    return value_ > time;
}

bool TimeLt::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ > start_time;
}

bool TimeLt::contain_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ > end_time;
}

std::vector<TimeRange*>* TimeLt::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    if (value_ != std::numeric_limits<int64_t>::min()) {
        result->push_back(
            new TimeRange(std::numeric_limits<int64_t>::min(), value_ - 1));
    }
    return result;
}

// TimeLtEq
TimeLtEq::TimeLtEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeLtEq::~TimeLtEq() {}

bool TimeLtEq::satisfy(Statistic* statistic) {
    return value_ >= statistic->start_time_;
}

bool TimeLtEq::satisfy(int64_t time, int64_t value) { return value_ >= time; }

bool TimeLtEq::satisfy(int64_t time, common::String value) {
    return value_ >= time;
}

bool TimeLtEq::satisfy_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ >= start_time;
}

bool TimeLtEq::contain_start_end_time(int64_t start_time, int64_t end_time) {
    return value_ >= end_time;
}

std::vector<TimeRange*>* TimeLtEq::get_time_ranges() {
    std::vector<TimeRange*>* result = new std::vector<TimeRange*>();
    result->push_back(
        new TimeRange(std::numeric_limits<int64_t>::min(), value_));
    return result;
}

// ============================================================================
// SIMD batch time filter implementations
// ============================================================================

// Helper: extract 4-bit movemask from 256-bit comparison result (4 x i64)
#if !defined(__ARM_NEON) && defined(ENABLE_SIMD)
static inline int simd_movemask_epi64(simde__m256i v) {
    // movemask_pd reinterprets as double and checks sign bit = high bit of each
    // 64-bit lane
    return simde_mm256_movemask_pd(simde_mm256_castsi256_pd(v));
}
#endif

int TimeGt::satisfy_batch_time(const int64_t* times, int count, bool* mask) {
    int pass = 0;
    int i = 0;
#if defined(__ARM_NEON)
    int64x2_t vval = vdupq_n_s64(value_);
    for (; i + 1 < count; i += 2) {
        int64x2_t vt = vld1q_s64(times + i);
        uint64x2_t cmp = vcgtq_s64(vt, vval);
        mask[i] = vgetq_lane_u64(cmp, 0) != 0;
        mask[i + 1] = vgetq_lane_u64(cmp, 1) != 0;
        pass += mask[i] + mask[i + 1];
    }
#elif defined(ENABLE_SIMD)
    simde__m256i vval = simde_mm256_set1_epi64x(value_);
    for (; i + 3 < count; i += 4) {
        simde__m256i vt =
            simde_mm256_loadu_si256((const simde__m256i*)(times + i));
        // time > value_ => cmpgt(time, value_)
        simde__m256i cmp = simde_mm256_cmpgt_epi64(vt, vval);
        int bits = simd_movemask_epi64(cmp);
        for (int j = 0; j < 4; ++j) {
            mask[i + j] = (bits >> j) & 1;
            pass += mask[i + j];
        }
    }
#endif
    for (; i < count; ++i) {
        mask[i] = value_ < times[i];
        if (mask[i]) ++pass;
    }
    return pass;
}

int TimeGtEq::satisfy_batch_time(const int64_t* times, int count, bool* mask) {
    int pass = 0;
    int i = 0;
#if defined(__ARM_NEON)
    int64x2_t vval = vdupq_n_s64(value_);
    for (; i + 1 < count; i += 2) {
        int64x2_t vt = vld1q_s64(times + i);
        uint64x2_t cmp = vcgeq_s64(vt, vval);
        mask[i] = vgetq_lane_u64(cmp, 0) != 0;
        mask[i + 1] = vgetq_lane_u64(cmp, 1) != 0;
        pass += mask[i] + mask[i + 1];
    }
#elif defined(ENABLE_SIMD)
    simde__m256i vval = simde_mm256_set1_epi64x(value_);
    for (; i + 3 < count; i += 4) {
        simde__m256i vt =
            simde_mm256_loadu_si256((const simde__m256i*)(times + i));
        // time >= value_ => NOT(cmpgt(value_, time))
        simde__m256i cmp = simde_mm256_cmpgt_epi64(vval, vt);
        simde__m256i ncmp =
            simde_mm256_xor_si256(cmp, simde_mm256_set1_epi64x((int64_t)-1));
        int bits = simd_movemask_epi64(ncmp);
        for (int j = 0; j < 4; ++j) {
            mask[i + j] = (bits >> j) & 1;
            pass += mask[i + j];
        }
    }
#endif
    for (; i < count; ++i) {
        mask[i] = value_ <= times[i];
        if (mask[i]) ++pass;
    }
    return pass;
}

int TimeLt::satisfy_batch_time(const int64_t* times, int count, bool* mask) {
    int pass = 0;
    int i = 0;
#if defined(__ARM_NEON)
    int64x2_t vval = vdupq_n_s64(value_);
    for (; i + 1 < count; i += 2) {
        int64x2_t vt = vld1q_s64(times + i);
        uint64x2_t cmp = vcltq_s64(vt, vval);
        mask[i] = vgetq_lane_u64(cmp, 0) != 0;
        mask[i + 1] = vgetq_lane_u64(cmp, 1) != 0;
        pass += mask[i] + mask[i + 1];
    }
#elif defined(ENABLE_SIMD)
    simde__m256i vval = simde_mm256_set1_epi64x(value_);
    for (; i + 3 < count; i += 4) {
        simde__m256i vt =
            simde_mm256_loadu_si256((const simde__m256i*)(times + i));
        // time < value_ => cmpgt(value_, time)
        simde__m256i cmp = simde_mm256_cmpgt_epi64(vval, vt);
        int bits = simd_movemask_epi64(cmp);
        for (int j = 0; j < 4; ++j) {
            mask[i + j] = (bits >> j) & 1;
            pass += mask[i + j];
        }
    }
#endif
    for (; i < count; ++i) {
        mask[i] = value_ > times[i];
        if (mask[i]) ++pass;
    }
    return pass;
}

int TimeLtEq::satisfy_batch_time(const int64_t* times, int count, bool* mask) {
    int pass = 0;
    int i = 0;
#if defined(__ARM_NEON)
    int64x2_t vval = vdupq_n_s64(value_);
    for (; i + 1 < count; i += 2) {
        int64x2_t vt = vld1q_s64(times + i);
        uint64x2_t cmp = vcleq_s64(vt, vval);
        mask[i] = vgetq_lane_u64(cmp, 0) != 0;
        mask[i + 1] = vgetq_lane_u64(cmp, 1) != 0;
        pass += mask[i] + mask[i + 1];
    }
#elif defined(ENABLE_SIMD)
    simde__m256i vval = simde_mm256_set1_epi64x(value_);
    for (; i + 3 < count; i += 4) {
        simde__m256i vt =
            simde_mm256_loadu_si256((const simde__m256i*)(times + i));
        // time <= value_ => NOT(cmpgt(time, value_))
        simde__m256i cmp = simde_mm256_cmpgt_epi64(vt, vval);
        simde__m256i ncmp =
            simde_mm256_xor_si256(cmp, simde_mm256_set1_epi64x((int64_t)-1));
        int bits = simd_movemask_epi64(ncmp);
        for (int j = 0; j < 4; ++j) {
            mask[i + j] = (bits >> j) & 1;
            pass += mask[i + j];
        }
    }
#endif
    for (; i < count; ++i) {
        mask[i] = value_ >= times[i];
        if (mask[i]) ++pass;
    }
    return pass;
}

int TimeEq::satisfy_batch_time(const int64_t* times, int count, bool* mask) {
    int pass = 0;
    int i = 0;
#if defined(__ARM_NEON)
    int64x2_t vval = vdupq_n_s64(value_);
    for (; i + 1 < count; i += 2) {
        int64x2_t vt = vld1q_s64(times + i);
        uint64x2_t cmp = vceqq_s64(vt, vval);
        mask[i] = vgetq_lane_u64(cmp, 0) != 0;
        mask[i + 1] = vgetq_lane_u64(cmp, 1) != 0;
        pass += mask[i] + mask[i + 1];
    }
#elif defined(ENABLE_SIMD)
    simde__m256i vval = simde_mm256_set1_epi64x(value_);
    for (; i + 3 < count; i += 4) {
        simde__m256i vt =
            simde_mm256_loadu_si256((const simde__m256i*)(times + i));
        simde__m256i cmp = simde_mm256_cmpeq_epi64(vt, vval);
        int bits = simd_movemask_epi64(cmp);
        for (int j = 0; j < 4; ++j) {
            mask[i + j] = (bits >> j) & 1;
            pass += mask[i + j];
        }
    }
#endif
    for (; i < count; ++i) {
        mask[i] = value_ == times[i];
        if (mask[i]) ++pass;
    }
    return pass;
}

int TimeNotEq::satisfy_batch_time(const int64_t* times, int count, bool* mask) {
    int pass = 0;
    int i = 0;
#if defined(__ARM_NEON)
    int64x2_t vval = vdupq_n_s64(value_);
    uint64x2_t ones = vdupq_n_u64(UINT64_MAX);
    for (; i + 1 < count; i += 2) {
        int64x2_t vt = vld1q_s64(times + i);
        uint64x2_t cmp = veorq_u64(vceqq_s64(vt, vval), ones);
        mask[i] = vgetq_lane_u64(cmp, 0) != 0;
        mask[i + 1] = vgetq_lane_u64(cmp, 1) != 0;
        pass += mask[i] + mask[i + 1];
    }
#elif defined(ENABLE_SIMD)
    simde__m256i vval = simde_mm256_set1_epi64x(value_);
    for (; i + 3 < count; i += 4) {
        simde__m256i vt =
            simde_mm256_loadu_si256((const simde__m256i*)(times + i));
        simde__m256i eq = simde_mm256_cmpeq_epi64(vt, vval);
        simde__m256i neq =
            simde_mm256_xor_si256(eq, simde_mm256_set1_epi64x((int64_t)-1));
        int bits = simd_movemask_epi64(neq);
        for (int j = 0; j < 4; ++j) {
            mask[i + j] = (bits >> j) & 1;
            pass += mask[i + j];
        }
    }
#endif
    for (; i < count; ++i) {
        mask[i] = value_ != times[i];
        if (mask[i]) ++pass;
    }
    return pass;
}

int TimeBetween::satisfy_batch_time(const int64_t* times, int count,
                                    bool* mask) {
    int pass = 0;
    int i = 0;
#if defined(__ARM_NEON)
    int64x2_t vlo = vdupq_n_s64(value1_);
    int64x2_t vhi = vdupq_n_s64(value2_);
    uint64x2_t ones = vdupq_n_u64(UINT64_MAX);
    for (; i + 1 < count; i += 2) {
        int64x2_t vt = vld1q_s64(times + i);
        uint64x2_t ge_lo = vcgeq_s64(vt, vlo);
        uint64x2_t le_hi = vcleq_s64(vt, vhi);
        uint64x2_t between = vandq_u64(ge_lo, le_hi);
        uint64x2_t result = not_ ? veorq_u64(between, ones) : between;
        mask[i] = vgetq_lane_u64(result, 0) != 0;
        mask[i + 1] = vgetq_lane_u64(result, 1) != 0;
        pass += mask[i] + mask[i + 1];
    }
#elif defined(ENABLE_SIMD)
    simde__m256i vlo = simde_mm256_set1_epi64x(value1_);
    simde__m256i vhi = simde_mm256_set1_epi64x(value2_);
    simde__m256i ones = simde_mm256_set1_epi64x((int64_t)-1);
    for (; i + 3 < count; i += 4) {
        simde__m256i vt =
            simde_mm256_loadu_si256((const simde__m256i*)(times + i));
        // time >= lo => NOT(cmpgt(lo, time))
        simde__m256i ge_lo =
            simde_mm256_xor_si256(simde_mm256_cmpgt_epi64(vlo, vt), ones);
        // time <= hi => NOT(cmpgt(time, hi))
        simde__m256i le_hi =
            simde_mm256_xor_si256(simde_mm256_cmpgt_epi64(vt, vhi), ones);
        simde__m256i between = simde_mm256_and_si256(ge_lo, le_hi);
        simde__m256i result =
            not_ ? simde_mm256_xor_si256(between, ones) : between;
        int bits = simd_movemask_epi64(result);
        for (int j = 0; j < 4; ++j) {
            mask[i + j] = (bits >> j) & 1;
            pass += mask[i + j];
        }
    }
#endif
    for (; i < count; ++i) {
        bool in_range = (value1_ <= times[i]) && (times[i] <= value2_);
        mask[i] = not_ ? !in_range : in_range;
        if (mask[i]) ++pass;
    }
    return pass;
}

}  // namespace storage
