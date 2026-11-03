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

#ifndef ENCODING_FIRE_H
#define ENCODING_FIRE_H

#include <cstdint>

template <typename T>
class Fire {
   public:
    explicit Fire(int learning_rate)
        : learn_shift_(learning_rate),
          bit_width_(0),
          accumulator_(0),
          delta_(0) {}

    virtual ~Fire() = default;

    virtual T predict(T value) = 0;

    virtual void train(T pre, T val, T err) = 0;

    virtual void reset() {
        accumulator_ = 0;
        delta_ = 0;
    }

   protected:
    int learn_shift_;
    int bit_width_;
    int accumulator_;
    T delta_;
};

class IntFire : public Fire<int> {
   public:
    explicit IntFire(int learning_rate) : Fire(learning_rate) {
        bit_width_ = 8;
        accumulator_ = 0;
        delta_ = 0;
    }

    void reset() override {
        accumulator_ = 0;
        delta_ = 0;
    }

    int predict(int value) override {
        int alpha = accumulator_ >> learn_shift_;
        int diff = static_cast<int32_t>(static_cast<uint32_t>(alpha) *
                                        static_cast<uint32_t>(delta_)) >>
                   bit_width_;
        return static_cast<int32_t>(static_cast<uint32_t>(value) +
                                    static_cast<uint32_t>(diff));
    }

    void train(int pre, int val, int err) override {
        uint32_t gradient = err > 0
                                ? uint32_t(0) - static_cast<uint32_t>(delta_)
                                : static_cast<uint32_t>(delta_);
        accumulator_ = static_cast<int32_t>(
            static_cast<uint32_t>(accumulator_) - gradient);
        delta_ = static_cast<int32_t>(static_cast<uint32_t>(val) -
                                      static_cast<uint32_t>(pre));
    }
};

class LongFire : public Fire<int64_t> {
   public:
    explicit LongFire(int learning_rate) : Fire(learning_rate) {
        bit_width_ = 16;
        accumulator_ = 0;
        delta_ = 0;
    }

    void reset() override {
        accumulator_ = 0;
        delta_ = 0;
    }

    int64_t predict(int64_t value) override {
        int64_t alpha = accumulator_ >> learn_shift_;
        // Java wraps the product to 64 bits before the arithmetic shift.
        int64_t diff = static_cast<int64_t>(static_cast<uint64_t>(alpha) *
                                            static_cast<uint64_t>(delta_)) >>
                       bit_width_;
        return static_cast<int64_t>(static_cast<uint64_t>(value) +
                                    static_cast<uint64_t>(diff));
    }

    void train(int64_t pre, int64_t val, int64_t err) override {
        uint64_t gradient = err > 0
                                ? uint64_t(0) - static_cast<uint64_t>(delta_)
                                : static_cast<uint64_t>(delta_);
        accumulator_ =
            static_cast<int32_t>(static_cast<uint32_t>(accumulator_) -
                                 static_cast<uint32_t>(gradient));
        delta_ = static_cast<int64_t>(static_cast<uint64_t>(val) -
                                      static_cast<uint64_t>(pre));
    }
};

#endif  // ENCODING_FIRE_H
