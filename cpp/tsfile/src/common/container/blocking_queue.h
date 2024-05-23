#ifndef COMMON_CONTAINER_BLOCKING_QUEUE_H
#define COMMON_CONTAINER_BLOCKING_QUEUE_H

#include <queue>
#include <pthread.h>

namespace timecho
{
namespace common
{

class BlockingQueue
{
public:
  BlockingQueue();
  ~BlockingQueue();

  void push(void *data);
  // if empty, blocking
  void* pop();

private:
  std::queue<void*> queue_;
  pthread_mutex_t   mutex_;
  pthread_cond_t    cond_;
};

} // end namespace common
} // end namespace timecho

#endif // COMMON_CONTAINER_BLOCKING_QUEUE_H

