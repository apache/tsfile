#include <stdio.h>
#include "page_arena.h"
#include <new> 

namespace timecho
{
namespace common
{

char *PageArena::alloc(uint32_t size)
{
  if (LIKELY(size <= page_size_)) {
    Page *cur_page = dummy_head_.next_;
    if (LIKELY(cur_page != nullptr)) {
      char *ret_ptr = cur_page->alloc(size);
      if (LIKELY(ret_ptr != nullptr)) {
        return ret_ptr;
      }
    }

    /*
     * cur_page is null OR can not alloc @size from cur_page,
     * then alloc new page
     */
    void *ptr = base_allocator_.alloc(page_size_ + sizeof(Page), mid_);
    if (UNLIKELY(ptr == nullptr)) {
      return nullptr;
    }
    Page *new_page = new(ptr) Page(page_size_, cur_page);
    dummy_head_.next_ = new_page;
    return new_page->alloc(size);
  } else {
    void *ptr = base_allocator_.alloc(size + sizeof(Page), mid_);
    Page *new_page = new(ptr) Page(dummy_head_.next_);
    dummy_head_.next_ = new_page;
    return reinterpret_cast<char*>(new_page) + sizeof(Page);
  }
}

void PageArena::reset()
{
  Page *p = dummy_head_.next_;
  while (p) {
    dummy_head_.next_ = p->next_;
    base_allocator_.free(p);
    p = dummy_head_.next_;
  }
}

} // end namespace common
} // end namespace timecho

