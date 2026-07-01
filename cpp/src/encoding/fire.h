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
        int diff = static_cast<int>((static_cast<int64_t>(alpha) * delta_)) >>
                   bit_width_;

        return value + diff;
    }

    void train(int pre, int val, int err) override {
        int gradient = err > 0 ? -delta_ : delta_;
        accumulator_ -= gradient;
        delta_ = val - pre;
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
        int64_t diff = (alpha * delta_) >> bit_width_;
        return value + diff;
    }

    void train(int64_t pre, int64_t val, int64_t err) override {
        int64_t gradient = err > 0 ? -delta_ : delta_;
        accumulator_ -= gradient;
        delta_ = val - pre;
    }
};

#endif  // ENCODING_FIRE_H
