
#include "blocking_queue.h"

namespace common {

BlockingQueue::BlockingQueue() : queue_(), mutex_(), cond_() {
    pthread_mutex_init(&mutex_, NULL);
    pthread_cond_init(&cond_, NULL);
}

BlockingQueue::~BlockingQueue() {
    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&cond_);
}

void BlockingQueue::push(void *data) {
    pthread_mutex_lock(&mutex_);
    queue_.push(data);
    pthread_mutex_unlock(&mutex_);
    /*
     * it is safe to signal after unlock.
     * since pthread_cond_wait is guarantee to unlock and sleep atomically.
     */
    pthread_cond_signal(&cond_);
}

void *BlockingQueue::pop() {
    void *ret_data = NULL;
    pthread_mutex_lock(&mutex_);
    while (queue_.empty()) {
        pthread_cond_wait(&cond_, &mutex_);
    }
    ret_data = queue_.front();
    queue_.pop();
    pthread_mutex_unlock(&mutex_);
    return ret_data;
}

}  // end namespace common