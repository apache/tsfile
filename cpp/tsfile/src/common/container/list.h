
#ifndef COMMON_CONTAINER_LIST_H
#define COMMON_CONTAINER_LIST_H

#include "common/allocator/page_arena.h"
#include "common/errno_define.h"

namespace timecho
{
namespace common
{

#define INVALID_NODE_PTR ((SimpleListNode*)0xABCDEF)

/*
 * A very simple list.
 * There are such scenarios:
 *   We prepare all data in the list, and pass the list as immutable
 *   parameters to other module or caller. We never do delete or complex
 *   push operations.
 *   for example:
 *     SchemaMgr::get_timeseries_by_tsid(TsID, list<ColumnDesc>&)
 *     SchemaMgr::create_aligned_timeseries(..., const list<char*> &measurement_list, ...)
 * 
 * Note: not thread safe!
 */
template<class T>
class SimpleList
{
private:
  struct SimpleListNode
  {
    T data_;
    SimpleListNode *next_;

    SimpleListNode() : next_(nullptr) {}
    SimpleListNode(const T &data) : data_(data), next_(nullptr) {}
  };

public:
  class Iterator
  {
  public:
    Iterator(SimpleListNode *node) : cur_(node) {}
    Iterator() : cur_(INVALID_NODE_PTR) {}
    T &get() { return cur_->data_; }
    FORCE_INLINE bool is_inited() const
    {
      return cur_ != INVALID_NODE_PTR;
    }
    FORCE_INLINE Iterator& operator++(int n)
    {
      if (LIKELY(cur_ != nullptr)) {
        cur_ = cur_->next_;
      }
      return *this;
    }
    FORCE_INLINE bool operator!=(const Iterator &other) const
    {
      return this->cur_ != other.cur_;
    }
    FORCE_INLINE bool operator==(const Iterator &other) const
    {
      return this->cur_ == other.cur_;
    }
  private:
    SimpleListNode *cur_;
  };

public:
  SimpleList(const uint32_t page_size, AllocModID mid) : head_(nullptr), tail_(nullptr), size_(0)
  {
    own_page_arena_.init(page_size, mid);
    page_arena_ = &own_page_arena_;
  }

  SimpleList(PageArena *page_arena) : page_arena_(page_arena), head_(nullptr), tail_(nullptr), size_(0)
  {
    // page_arena_ should be destroy outside by caller
  }

  int push_back(const T &data)
  {
    void *buf = page_arena_->alloc(sizeof(SimpleListNode));
    if (UNLIKELY(buf == nullptr)) {
      return common::E_OOM;
    }
    SimpleListNode *node = new(buf) SimpleListNode(data);
    if (head_ == nullptr) {
      head_ = node;
      tail_ = node;
    } else {
      assert(tail_ != nullptr);
      tail_->next_ = node;
      tail_ = node;
    }
    size_++;
    return common::E_OK;
  }

  FORCE_INLINE T& front()
  {
    ASSERT(size_ > 0 && head_ != nullptr);
    return head_->data_;
  }

  int remove(T target)
  {
    if (head_ == nullptr) {
      return common::E_NOT_EXIST;
    }
    SimpleListNode *prev = head_;
    SimpleListNode *cur = head_->next_;
    while (cur && cur->data_ != target) {
      cur = cur->next_;
    }
    if (cur) {
      return common::E_NOT_EXIST;
    }
    prev->next_ = cur->next_;
    // cur is allocated from PageArena, it should not reclaim now
    return common::E_OK;
  }

  FORCE_INLINE Iterator begin() const { return Iterator(head_); }
  FORCE_INLINE Iterator end() const { return Iterator(nullptr); }
  FORCE_INLINE uint32_t size() const { return size_; }
  FORCE_INLINE void clear()
  {
    head_ = nullptr;
    tail_ = nullptr;
    size_ = 0;
    own_page_arena_.destroy();
  }
private:
  PageArena *page_arena_;
  PageArena own_page_arena_;
  SimpleListNode *head_;
  SimpleListNode *tail_;
  uint32_t size_;
};

} // end namespace common
} // end namespace timecho

#endif // COMMON_CONTAINER_LIST_H

