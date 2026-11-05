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
#ifndef COMMON_SIMD_DISPATCH_H
#define COMMON_SIMD_DISPATCH_H

#include "common/global.h"

// Runtime SIMD dispatch.
//
// The library is built for the portable x86-64 baseline (SSE2 only), so a
// kernel written with simde's AVX2/SSSE3 API is compiled into simde's portable
// fallback — measured ~6x slower than real AVX2 for the RLE bit-unpack. To run
// the fast instructions without raising the binary's ISA floor (an -mavx2
// binary faults on older CPUs), such kernels get a second copy annotated with
// __attribute__((target("avx2"))) that uses raw intrinsics, and call sites pick
// it at runtime via cpu_has_avx2().
//
// Only GCC/Clang on x86 provide both pieces; elsewhere callers keep the
// portable path.

#if defined(__x86_64__) || defined(__i386__)
#if defined(__GNUC__) || defined(__clang__)
#define TSFILE_X86_AVX2_DISPATCH 1
#define TSFILE_TARGET_AVX2 __attribute__((target("avx2")))
#endif
#endif

namespace common {

#ifdef TSFILE_X86_AVX2_DISPATCH
// True when the running CPU implements AVX2. Cached after the first call;
// glibc/libgcc initialize the CPU feature bits before any regular function
// runs, so no explicit __builtin_cpu_init() is required here.
inline bool cpu_has_avx2() {
    static const bool has_avx2 = __builtin_cpu_supports("avx2") != 0;
    return has_avx2;
}
#endif

}  // namespace common

#endif  // COMMON_SIMD_DISPATCH_H
