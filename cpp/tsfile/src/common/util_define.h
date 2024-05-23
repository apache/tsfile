
/*
 * This file defines some basic macros
 */

#ifndef COMMON_UTIL_DEFINE_H
#define COMMON_UTIL_DEFINE_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

/* ======== unsued ======== */
#define UNUSED(v) ((void)(v))


/* ======== inline ======== */
#ifdef __GNUC__
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE inline
#endif // __GNUC__

#ifdef BUILD_FOR_SMALL_BINARY
#define INLINE FORCE_INLINE
#else
#define INLINE
#endif // BUILD_FOR_SMALL_BINARY


/* ======== likely ======== */
#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif // __GNUC__ >= 4


/* ======== nullptr ======== */
#if __cplusplus < 201103L
#ifndef nullptr
#define nullptr NULL
#endif
#define OVERRIDE 
#else
#define OVERRIDE override
#endif // __cplusplus < 201103L


/* ======== cache line ======== */
#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif // CACHE_LINE_SIZE


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
#define STATIC_ASSERT(cond, msg) typedef char static_assertion_##msg[(cond) ? 1 : -1] __attribute__((unused))
#else
#define STATIC_ASSERT(cond, msg) static_assert((cond), #msg)
#endif // __cplusplus < 201103L


/* ======== atomic operation ======== */
#define ATOMIC_FAA(val_addr, addv)  __atomic_fetch_add((val_addr), (addv), __ATOMIC_SEQ_CST)
#define ATOMIC_AAF(val_addr, addv)  __atomic_add_fetch((val_addr), (addv), __ATOMIC_SEQ_CST)
/*
 * It implements an atomic compare and exchange operation.
 * This compares the contents of *ptr with the contents of *expected.
 * - If equal, the operation is a read-modify-write operation that writes desired into *ptr.
 * - If they are not equal, the operation is a read and the current contents of *ptr are written into *expected
 */
#define ATOMIC_CAS(val_addr, expected, desired) \
  __atomic_compare_exchange_n((val_addr), (expected),(desired), \
                              /* weak = */ false, \
                              /* success_memorder = */ __ATOMIC_SEQ_CST, \
                              /* failure_memorder = */ __ATOMIC_SEQ_CST)
#define ATOMIC_LOAD(val_addr)  __atomic_load_n((val_addr), __ATOMIC_SEQ_CST)
#define ATOMIC_STORE(val_addr, val) __atomic_store_n((val_addr), (val), __ATOMIC_SEQ_CST)

/* ======== align ======== */
#define ALIGNED(a) __attribute__((aligned(a)))
#define ALIGNED_4 ALIGNED(4)
#define ALIGNED_8 ALIGNED(8)

/* ======== disallow copy and assign ======== */
#if __cplusplus < 201103L
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)
#else
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete; \
  TypeName& operator=(const TypeName&) = delete;
#endif

/* ======== return value check ======== */
#define RET_FAIL(expr) UNLIKELY(common::E_OK != (ret = (expr)))
#define RFAIL(expr) UNLIKELY(common::E_OK != (ret = (expr)))
#define RET_SUCC(expr) LIKELY(common::E_OK != (ret = (exprt)))
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

#endif // COMMON_UTIL_DEFINE_H

