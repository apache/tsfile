#ifndef COMMON_CONTAINER_BLOCKING_QUEUE_H
#define COMMON_CONTAINER_BLOCKING_QUEUE_H

#include <pthread.h>

#include <queue>

namespace common {

class BlockingQueue {
   public:
    BlockingQueue();
    ~BlockingQueue();

    void push(void* data);
    // if empty, blocking
    void* pop();

   private:
    std::queue<void*> queue_;
    pthread_mutex_t mutex_;
    pthread_cond_t cond_;
};

}  // end namespace common
#endif  // COMMON_CONTAINER_BLOCKING_QUEUE_H
