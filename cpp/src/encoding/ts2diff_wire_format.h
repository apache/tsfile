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

#ifndef ENCODING_TS2DIFF_WIRE_FORMAT_H
#define ENCODING_TS2DIFF_WIRE_FORMAT_H

#include <cstdint>

namespace storage {
namespace ts2diff_java_detail {

// FLOAT/DOUBLE TS_2DIFF page markers, shared by the encoder and the decoder
// so a single definition describes the wire format.  See
// cpp/docs/ts2diff-float-double-wire-format.md.
constexpr uint32_t FLAG_ORIGINAL_VALUE_OVERFLOW =
    2147483646u;  // Integer.MAX_VALUE - 1
constexpr uint32_t FLAG_SCALED_VALUE_OVERFLOW =
    2147483647u;  // Integer.MAX_VALUE

}  // namespace ts2diff_java_detail
}  // namespace storage

#endif  // ENCODING_TS2DIFF_WIRE_FORMAT_H
