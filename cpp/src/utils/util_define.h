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
#ifndef UTILS_UTIL_DEFINE_H
#define UTILS_UTIL_DEFINE_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

/* ======== platform compatibility ========
 *
 * MSVC does not provide several POSIX types/functions/macros used across the
 * codebase. Provide drop-in equivalents so the same source compiles on both
 * GCC/Clang (Linux) and MSVC (Windows) without scattering #ifdefs.
 */
#ifdef _WIN32
#include <io.h>
#include <string.h>

#if defined(_MSC_VER)
// ssize_t is a signed, pointer-sized integer; intptr_t (from <stdint.h>,
// included above) is exactly that. We deliberately avoid <BaseTsd.h>/SSIZE_T
// because that header also pollutes the global namespace with INT32/INT64
// typedefs, which collide with the project's own INT32/INT64 enum values.
typedef intptr_t ssize_t;
typedef int mode_t;
#endif  // _MSC_VER

// access() mode flags (POSIX <unistd.h>); MSVC's _access uses the same bits.
#ifndef F_OK
#define F_OK 0
#endif
#ifndef X_OK
#define X_OK 1
#endif
#ifndef W_OK
#define W_OK 2
#endif
#ifndef R_OK
#define R_OK 4
#endif

#ifndef strcasecmp
#define strcasecmp _stricmp
#endif
#ifndef strncasecmp
#define strncasecmp _strnicmp
#endif
#endif  // _WIN32

/* ======== shared-library symbol visibility ========
 *
 * Functions are exported from tsfile.dll automatically via
 * CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS, but global DATA symbols (plain variables,
 * static class members) are not reliably auto-exported, and a consumer must
 * see __declspec(dllimport) to reference them across the DLL boundary. Mark
 * such symbols with TSFILE_API: it expands to dllexport while building the
 * library (TSFILE_BUILDING is defined for its own translation units),
 * dllimport for external consumers, and nothing on non-MSVC toolchains.
 */
#if defined(_MSC_VER)
#if defined(TSFILE_BUILDING)
#define TSFILE_API __declspec(dllexport)
#else
#define TSFILE_API __declspec(dllimport)
#endif
#else
#define TSFILE_API
#endif

/* ======== unused ======== */
#define UNUSED(v) ((void)(v))
#if __cplusplus >= 201703L
#define MAYBE_UNUSED [[maybe_unused]]
#elif defined(__GNUC__) || defined(__clang__)
#define MAYBE_UNUSED __attribute__((unused))
#else
#define MAYBE_UNUSED
#endif

/* ======== inline ======== */
#if (defined(__GNUC__) || defined(__clang__)) && !defined(__MINGW32__)
#define FORCE_INLINE inline __attribute__((always_inline))
#elif defined(_MSC_VER)
#define FORCE_INLINE __forceinline
#else
#define FORCE_INLINE inline
#endif

#ifdef BUILD_FOR_SMALL_BINARY
#define INLINE FORCE_INLINE
#else
#define INLINE
#endif  // BUILD_FOR_SMALL_BINARY

/* ======== likely ======== */
#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif  // __GNUC__ >= 4

/* ======== nullptr ======== */
#if __cplusplus < 201103L
#ifndef nullptr
#define nullptr NULL
#endif
#define OVERRIDE
#else
#define OVERRIDE override
#endif  // __cplusplus < 201103L

/* ======== cache line ======== */
#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif  // CACHE_LINE_SIZE

/* ======== assert ======== */
#ifdef NDEBUG
#define ASSERT(condition) ((void)0)
#else
#define ASSERT(condition) assert((condition))
#endif  // NDEBUG

/* ======== statis assert ======== */
/*
 * To be compatible with C++ before C++11,
 * @msg should be a single word (use -/_ to concat)
 * such as This_should_be_TRUE
 */
#if __cplusplus < 201103L
// TODO only define this when DEBUG
#define STATIC_ASSERT(cond, msg) \
    typedef char static_assertion_##msg[(cond) ? 1 : -1] __attribute__((unused))
#else
#define STATIC_ASSERT(cond, msg) static_assert((cond), #msg)
#endif  // __cplusplus < 201103L

/* ======== atomic operation ========
 *
 * The ATOMIC_* macros operate on the address of a plain (non-std::atomic)
 * scalar, matching the semantics of the GCC/Clang __atomic builtins.
 *
 * - On GCC/Clang the builtins are used directly (unchanged behaviour).
 * - On other compilers (MSVC) they are implemented on top of C++11 <atomic>
 *   via helper templates. Reinterpreting a plain scalar's address as a
 *   std::atomic<T>* is well-defined in practice for lock-free integral types
 *   (this is exactly what C++20 std::atomic_ref formalizes); all current call
 *   sites use naturally-aligned integral members.
 */
#if defined(__GNUC__) || defined(__clang__)
#define ATOMIC_FAA(val_addr, addv) \
    __atomic_fetch_add((val_addr), (addv), __ATOMIC_SEQ_CST)
#define ATOMIC_AAF(val_addr, addv) \
    __atomic_add_fetch((val_addr), (addv), __ATOMIC_SEQ_CST)
/*
 * It implements an atomic compare and exchange operation.
 * This compares the contents of *ptr with the contents of *expected.
 * - If equal, the operation is a reader-modify-writer operation that writes
 * desired into *ptr.
 * - If they are not equal, the operation is a reader and the current contents
 * of *ptr are written into *expected
 */
#define ATOMIC_CAS(val_addr, expected, desired)                            \
    __atomic_compare_exchange_n((val_addr), (expected), (desired),         \
                                /* weak = */ false,                        \
                                /* success_memorder = */ __ATOMIC_SEQ_CST, \
                                /* failure_memorder = */ __ATOMIC_SEQ_CST)
#define ATOMIC_LOAD(val_addr) __atomic_load_n((val_addr), __ATOMIC_SEQ_CST)
#define ATOMIC_STORE(val_addr, val) \
    __atomic_store_n((val_addr), (val), __ATOMIC_SEQ_CST)
#elif defined(__cplusplus)
#include <atomic>
namespace common {
namespace util_atomic {
template <typename T>
inline std::atomic<T>* as_atomic(T* p) {
    return reinterpret_cast<std::atomic<T>*>(p);
}
template <typename T>
inline const std::atomic<T>* as_atomic(const T* p) {
    return reinterpret_cast<const std::atomic<T>*>(p);
}
// fetch-and-add: returns the value held *before* the addition.
template <typename T, typename V>
inline T faa(T* p, V v) {
    return as_atomic(p)->fetch_add(static_cast<T>(v),
                                   std::memory_order_seq_cst);
}
// add-and-fetch: returns the value held *after* the addition.
template <typename T, typename V>
inline T aaf(T* p, V v) {
    return static_cast<T>(
        as_atomic(p)->fetch_add(static_cast<T>(v), std::memory_order_seq_cst) +
        static_cast<T>(v));
}
// compare-and-swap: returns true on success; on failure writes the current
// value into *expected (same contract as __atomic_compare_exchange_n).
template <typename T, typename D>
inline bool cas(T* p, T* expected, D desired) {
    return as_atomic(p)->compare_exchange_strong(
        *expected, static_cast<T>(desired), std::memory_order_seq_cst);
}
template <typename T>
inline T load(const T* p) {
    return as_atomic(p)->load(std::memory_order_seq_cst);
}
template <typename T, typename V>
inline void store(T* p, V v) {
    as_atomic(p)->store(static_cast<T>(v), std::memory_order_seq_cst);
}
}  // namespace util_atomic
}  // namespace common
#define ATOMIC_FAA(val_addr, addv) \
    (::common::util_atomic::faa((val_addr), (addv)))
#define ATOMIC_AAF(val_addr, addv) \
    (::common::util_atomic::aaf((val_addr), (addv)))
#define ATOMIC_CAS(val_addr, expected, desired) \
    (::common::util_atomic::cas((val_addr), (expected), (desired)))
#define ATOMIC_LOAD(val_addr) (::common::util_atomic::load((val_addr)))
#define ATOMIC_STORE(val_addr, val) \
    (::common::util_atomic::store((val_addr), (val)))
#endif  // atomic operation

/* ======== align ======== */
#if defined(__GNUC__) || defined(__clang__)
#define ALIGNED(a) __attribute__((aligned(a)))
#elif defined(_MSC_VER)
#define ALIGNED(a) __declspec(align(a))
#else
#define ALIGNED(a)
#endif
#define ALIGNED_4 ALIGNED(4)
#define ALIGNED_8 ALIGNED(8)

/* ======== disallow copy and assign ======== */
#if __cplusplus < 201103L
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);             \
    void operator=(const TypeName&)
#else
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&) = delete;    \
    TypeName& operator=(const TypeName&) = delete;
#endif

/* ======== return value check ======== */
#define RET_FAIL(expr) UNLIKELY(common::E_OK != (ret = (expr)))
#define RFAIL(expr) UNLIKELY(common::E_OK != (ret = (expr)))
#define RET_SUCC(expr) LIKELY(common::E_OK == (ret = (expr)))
#define RSUCC(expr) LIKELY(common::E_OK != (ret = (exprt)))
#define IS_SUCC(ret) LIKELY(common::E_OK == (ret))
#define IS_FAIL(ret) UNLIKELY(common::E_OK != (ret))

#define IS_NULL(ptr) UNLIKELY((ptr) == nullptr)

/* ======== min/max ======== */
#define UTIL_MAX(a, b) ((a) > (b) ? (a) : (b))
#define UTIL_MIN(a, b) ((a) > (b) ? (b) : (a))

/*
 * int64_max < 10^20
 * consider +/- and the '\0' tail. 24 is enough
 */
#define INT64_TO_BASE10_MAX_LEN 24

#endif  // UTILS_UTIL_DEFINE_H
