
#ifndef COMMON_ALLOCTOR_OBJECT_POOL_H
#define COMMON_ALLOCTOR_OBJECT_POOL_H

#include "common/allocator/alloc_base.h"
#include "common/mutex/mutex.h"

namespace timecho
{
namespace common
{

template<class T>
class ObjectPool
{
private:
  struct ObjectPoolNode
  {
    T data_;
    ObjectPoolNode *next_;

    ObjectPoolNode() : data_(), next_(nullptr) {}
  };

public:
  /*
   * max_cache_count is a soft limitation:
   */
  ObjectPool(const uint32_t max_cache_count,
             const AllocModID mid,
             BaseAllocator &allocator = g_base_allocator)
    : max_cache_count_(max_cache_count),
      cur_alloc_count_(0),
      mid_(mid),
      allocator_(allocator),
      mutex_(),
      head_(nullptr)
  {
    assert(max_cache_count > 1);
  }

  ~ObjectPool() { destroy(); }

  void destroy()
  {
    ObjectPoolNode *cur = head_;
    while (cur) {
      head_ = cur->next_;
      allocator_.free(cur);
      cur = head_;
      cur_alloc_count_--;
    }
    ASSERT(cur_alloc_count_ == 0);
  }

  T *alloc()
  {
    T *ret_obj = nullptr;
    common::MutexGuard g(mutex_);
    if (head_) {
      ret_obj = &(head_->data_);
      head_ = head_->next_;
      return ret_obj;
    } else {
      void *buf = allocator_.alloc(sizeof(ObjectPoolNode), mid_);
      if (UNLIKELY(buf == nullptr)) {
        return nullptr;
      }
      cur_alloc_count_++;
      ret_obj = &(new (buf) ObjectPoolNode)->data_;
      return ret_obj;
    }
  }

  void free(T* obj)
  {
    ASSERT(obj != nullptr);
    common::MutexGuard g(mutex_);
    if (cur_alloc_count_ > max_cache_count_) {
      allocator_.free(obj);
      cur_alloc_count_--;
      ASSERT(cur_alloc_count_ >= 0);
    } else {
      ObjectPoolNode *n = (ObjectPoolNode*)obj;
      n->next_ = head_;
      head_ = n;
    }
  }

  uint32_t get_cur_alloc_count() const { return cur_alloc_count_; }
private:
  uint32_t max_cache_count_;
  uint32_t cur_alloc_count_;
  AllocModID mid_;
  BaseAllocator allocator_;
  common::Mutex mutex_;
  ObjectPoolNode *head_; // freelist head
};

} // end namspace common
} // end namespace timecho

#endif // COMMON_ALLOCTOR_OBJECT_POOL_H

