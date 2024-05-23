
#ifndef COMMON_ALLOCATOR_STL_ALLOCATOR_H
#define COMMON_ALLOCATOR_STL_ALLOCATOR_H

#include "alloc_base.h"

namespace common {

template <class T, AllocModID Mid, class TAllocator = BaseAllocator>
class StlAllocator {
   public:
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;
    typedef T *pointer;
    typedef const T *const_pointer;
    typedef T &reference;
    typedef const T &const_reference;
    typedef T value_type;

    /*
     * rebind provides a way to obtain an allocator for a different type.
     * For example, std::list will alloc object Node<T> beside alloc object T.
     */
    template <class U>
    struct rebind {
        typedef StlAllocator<U, Mid, TAllocator> other;
    };

    StlAllocator() {}
    StlAllocator(const StlAllocator &) {}

    template <class T2, AllocModID Mid2>
    StlAllocator(const StlAllocator<T2, Mid2, TAllocator> &) {}

    StlAllocator(TAllocator base_allocator) : base_allocator_(base_allocator) {}

    pointer address(reference x) const { return &x; }
    const_pointer address(const_reference x) { return &x; }

    pointer allocate(size_type n, const void *hint = 0) {
        return (pointer)base_allocator_.alloc(n * sizeof(T), Mid);
    };
    void deallocate(void *p, size_type) { base_allocator_.free(p); }
    size_type max_size() const { return uint32_t(-1); }

    void construct(pointer p, const T &val) { new ((T *)p) T(val); }
    void destroy(pointer p) { p->~T(); }

   private:
    TAllocator base_allocator_;
};

/*
 * According to the manual, allocator is stateless.
 * Although we define a base_allocator_ here, but base_allocator_ is also
 * stateless. so '==' is always true and '!=' is always false. refer to
 * https://en.cppreference.com/w/cpp/memory/allocator/operator_cmp.
 */
template <class T1, AllocModID Mid1, class T2, AllocModID Mid2>
bool operator==(const StlAllocator<T1, Mid1> &a1,
                const StlAllocator<T2, Mid2> &a2) {
    return true;
}

template <class T1, AllocModID Mid1, class T2, AllocModID Mid2>
bool operator!=(const StlAllocator<T1, Mid1> &a1,
                const StlAllocator<T2, Mid2> &a2) {
    return false;
}

}  // end namespace common
#endif  // COMMON_ALLOCATOR_STL_ALLOCATOR_H
