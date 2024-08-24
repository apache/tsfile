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

#ifndef COMMON_ALLOCATOR_BYTE_STREAM_H
#define COMMON_ALLOCATOR_BYTE_STREAM_H

#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <string>

#include "common/allocator/alloc_base.h"
#include "common/allocator/my_string.h"
#include "utils/errno_define.h"

namespace common {

template <typename T>
class OptionalAtomic {
   public:
    OptionalAtomic(T t, bool enable_atomic = false)
        : val_(t), enable_atomic_(enable_atomic) {}

    FORCE_INLINE T load() const {
        if (UNLIKELY(enable_atomic_)) {
            return ATOMIC_LOAD(&val_);
        } else {
            return val_;
        }
    }

    FORCE_INLINE void store(const T t) {
        if (UNLIKELY(enable_atomic_)) {
            ATOMIC_STORE(&val_, t);
        } else {
            val_ = t;
        }
    }

    FORCE_INLINE T atomic_faa(const T increament) {
        if (UNLIKELY(enable_atomic_)) {
            return ATOMIC_FAA(&val_, increament);
        } else {
            T old_val = val_;
            val_ = val_ + increament;
            return old_val;
        }
    }

    FORCE_INLINE T atomic_aaf(const T increament) {
        if (UNLIKELY(enable_atomic_)) {
            return ATOMIC_AAF(&val_, increament);
        } else {
            val_ = val_ + increament;
            return val_;
        }
    }

    FORCE_INLINE bool enable_atomic() const { return enable_atomic_; }

   private:
    T val_;
    bool enable_atomic_;
};

FORCE_INLINE int32_t float_to_int(float f) {
    // return *((int32_t*)(&f));
    // return *(reinterpret_cast<int32_t*>(&f));
    union fi {
        int i_;
        float f_;
    };

    fi my_fi;
    my_fi.f_ = f;
    return my_fi.i_;  // By mimicking the java jdk's implementation, it's
                      // essentially the same as line 75.
}

FORCE_INLINE float int_to_float(int32_t i) {
    // return *((float *)(&i));
    union fi {
        int i_;
        float f_;
    };

    fi my_fi;
    my_fi.i_ = i;
    return my_fi.f_;
}

FORCE_INLINE void float_to_bytes(float f, uint8_t bytes[4]) {
    /*
     * See:
     * floatToBytes in BytesUtils.java of IoTDB project and
     * Java_java_lang_Float_intBitsToFloat in JDK project
     */
    if (UNLIKELY(f != f)) {  // this is NaN
        // IEEE754: 0x7FC00000 for NanN
        bytes[0] = 0x7F;
        bytes[1] = 0xC0;
        bytes[2] = 0x00;
        bytes[3] = 0x00;
        return;
    }

    // follow jdk implementation style.
    union {
        int i;
        float f;
    } u;

    u.f = f;

    bytes[3] = (uint8_t)(u.i);
    bytes[2] = (uint8_t)(u.i >> 8);
    bytes[1] = (uint8_t)(u.i >> 16);
    bytes[0] = (uint8_t)(u.i >> 24);
}

FORCE_INLINE float bytes_to_float(uint8_t bytes[4]) {
    int32_t i = bytes[3];
    i &= 0xFF;
    i |= bytes[2] << 8;
    i &= 0xFFFF;
    i |= bytes[1] << 16;
    i &= 0xFFFFFF;
    i |= bytes[0] << 24;

    union {
        int i;
        float f;
    } u;

    u.i = i;
    return u.f;
}

FORCE_INLINE int64_t double_to_long(double d) {
    // return *((int64_t*)(&d));
    union dl {
        double d_;
        int64_t l_;
    };
    dl my_dl;
    my_dl.d_ = d;
    return my_dl.l_;
}

FORCE_INLINE double long_to_double(int64_t l) {
    // return *((double *)(&l));
    union dl {
        double d_;
        int64_t l_;
    };
    dl my_dl;
    my_dl.l_ = l;
    return my_dl.d_;
}

FORCE_INLINE void double_to_bytes(double d, uint8_t bytes[8]) {
    if (UNLIKELY(d != d)) {
        // NaN, 0x7FF8000000000000L
        memset(bytes, 0, 8);
        bytes[0] = 0x7F;
        bytes[1] = 0xF8;
        return;
    }
    // follow jdk implementation style.
    union {
        long long l;
        double d;
    } u;

    u.d = d;

    bytes[7] = (uint8_t)u.l;
    bytes[6] = (uint8_t)(u.l >> 8);
    bytes[5] = (uint8_t)(u.l >> 16);
    bytes[4] = (uint8_t)(u.l >> 24);
    bytes[3] = (uint8_t)(u.l >> 32);
    bytes[2] = (uint8_t)(u.l >> 40);
    bytes[1] = (uint8_t)(u.l >> 48);
    bytes[0] = (uint8_t)(u.l >> 56);
}

FORCE_INLINE double bytes_to_double(uint8_t bytes[8]) {
    int64_t value = bytes[7];
    value &= 0xFFl;
    value |= ((int64_t)bytes[6]) << 8;
    value &= 0xFFFFl;
    value |= ((int64_t)bytes[5]) << 16;
    value &= 0xFFFFFFl;
    value |= ((int64_t)bytes[4]) << 24;
    value &= 0xFFFFFFFFl;
    value |= ((int64_t)bytes[3]) << 32;
    value &= 0xFFFFFFFFFFl;
    value |= ((int64_t)bytes[2]) << 40;
    value &= 0xFFFFFFFFFFFFl;
    value |= ((int64_t)bytes[1]) << 48;
    value &= 0xFFFFFFFFFFFFFFl;
    value |= ((int64_t)bytes[0]) << 56;

    // follow jdk implementation style.
    union {
        long long l;
        double d;
    } u;

    u.l = value;
    return u.d;
}

// TODO define a WrappedByteStream class

// auto extend buffer for serialization
class ByteStream {
   private:
    struct Page {
        OptionalAtomic<Page *> next_;  // 9 bytes
        uint8_t *buf_;                 // 8 bytes

        explicit Page(bool enable_atomic) : next_(nullptr, enable_atomic) {
            buf_ = (uint8_t
                        *)(this +
                           1);  // I think it should add 17, because the Page
                                // class is 9+8=17 bytes. No, adding one to a
                                // pointer is not adding a byte, but adding the
                                // length of the byte corresponding to the type
                                // pointed to by the pointer, and adding one
                                // here is actually adding 17 bytes. No problem.
        }
        Page(bool enable_atomic, uint8_t *wrapped_buf)
            : next_(nullptr, enable_atomic), buf_(wrapped_buf) {}
    };

   public:
    ByteStream(uint32_t page_size, AllocModID mid, bool enable_atomic = false,
               BaseAllocator &allocator = g_base_allocator)
        : allocator_(allocator),
          head_(nullptr, enable_atomic),
          tail_(nullptr, enable_atomic),
          read_page_(nullptr),
          total_size_(0, enable_atomic),
          read_pos_(0),
          marked_read_pos_(0),
          page_size_(page_size),
          mid_(mid),
          wrapped_page_(false, nullptr) {
        // assert(page_size >= 16);  // commented out by gxh on 2023.03.09
    }

    // TODO use a specific construct function to mark it as wrapped use.
    // for wrap plain buffer to ByteStream
    ByteStream()
        : allocator_(g_base_allocator),
          head_(nullptr, false),
          tail_(nullptr, false),
          read_page_(nullptr),
          total_size_(0, false),
          read_pos_(0),
          marked_read_pos_(0),
          page_size_(0),
          mid_(MOD_DEFAULT),
          wrapped_page_(false, nullptr) {}

    ~ByteStream() { destroy(); }

    /* ================ Part 0: wrap from outside buffer ================ */
    // if you wrap a buffer as a ByteStream, you should
    // manage the outside buffer yourself.
    void wrap_from(const char *buf, int32_t buf_len) {
        wrapped_page_.next_.store(nullptr);
        wrapped_page_.buf_ = (uint8_t *)buf;

        page_size_ = buf_len;
        head_.store(&wrapped_page_);
        tail_.store(&wrapped_page_);
        total_size_.store(buf_len);
        marked_read_pos_ = 0;
        read_pos_ = 0;
        read_page_ = nullptr;
    }
    FORCE_INLINE bool is_wrapped() const {
        return head_.load() == &wrapped_page_;
    }
    char *get_wrapped_buf() { return (char *)wrapped_page_.buf_; }
    void clear_wrapped_buf() { wrapped_page_.buf_ = nullptr; }

    /* ================ Part 1: basic ================ */
    FORCE_INLINE uint32_t remaining_size() const {
        ASSERT(total_size_.load() >= read_pos_);
        return total_size_.load() - read_pos_;
    }
    FORCE_INLINE bool has_remaining() const { return remaining_size() > 0; }

    FORCE_INLINE void mark_read_pos() { marked_read_pos_ = read_pos_; }
    FORCE_INLINE uint32_t get_mark_len() const {
        ASSERT(marked_read_pos_ <= read_pos_);
        return read_pos_ - marked_read_pos_;
    }

    void destroy() { reset(); }
    void reset() {
        // if this ByteStream is wrapped from a plain buffer
        // do not free the outside buffer.
        if (!is_wrapped()) {
            Page *p = head_.load();
            while (p) {
                p = head_.load()->next_.load();
                allocator_.free(head_.load());
                head_.store(p);
            }
        }
        head_.store(nullptr);
        tail_.store(nullptr);
        read_page_ = nullptr;
        total_size_.store(0);
        read_pos_ = 0;
    }

    // never used TODO
    void shallow_clone_from(ByteStream &other) {
        this->page_size_ = other.page_size_;
        this->mid_ = other.mid_;
        this->head_.store(other.head_.load());
        this->tail_.store(other.tail_.load());
        this->total_size_.store(other.total_size_.load());
    }

    FORCE_INLINE uint32_t total_size() const { return total_size_.load(); }
    FORCE_INLINE uint32_t read_pos() const { return read_pos_; };
    FORCE_INLINE void wrapped_buf_advance_read_pos(uint32_t size) {
        if (size + read_pos_ > total_size_.load()) {
            read_pos_ = total_size_.load();
        } else {
            read_pos_ += size;
        }
    }

    /* ================ Part 2: write_xxx and read_xxx ================ */
    // writer @buf with length @len into this bytestream
    int write_buf(const uint8_t *buf, const uint32_t len) {
        int ret = common::E_OK;
        uint32_t write_len = 0;
        while (write_len < len) {
            if (RET_FAIL(prepare_space())) {
                std::cout << "write_buf error " << ret << std::endl;
                return ret;
            }
            uint32_t remainder = page_size_ - (total_size_.load() % page_size_);
            uint32_t copy_len =
                remainder < (len - write_len) ? remainder : (len - write_len);
            memcpy(tail_.load()->buf_ + total_size_.load() % page_size_,
                   buf + write_len, copy_len);
            total_size_.atomic_aaf(copy_len);
            write_len += copy_len;
        }
        return ret;
    }

    // reader @want_len bytes to @buf, @read_len indicates real len we reader.
    // if ByteStream do not have so many bytes, it will return E_PARTIAL_READ if
    // no other error occure.
    int read_buf(uint8_t *buf, const uint32_t want_len, uint32_t &read_len) {
        int ret = common::E_OK;
        bool partial_read = (read_pos_ + want_len > total_size_.load());
        uint32_t want_len_limited =
            partial_read ? (total_size_.load() - read_pos_) : want_len;
        read_len = 0;
        while (read_len < want_len_limited) {
            if (RET_FAIL(check_space())) {
                return ret;
            }
            uint32_t remainder = page_size_ - (read_pos_ % page_size_);
            uint32_t copy_len = remainder < want_len_limited - read_len
                                    ? remainder
                                    : want_len_limited - read_len;
            memcpy(buf + read_len, read_page_->buf_ + (read_pos_ % page_size_),
                   copy_len);
            read_len += copy_len;
            read_pos_ += copy_len;
        }
        return partial_read ? common::E_PARTIAL_READ : common::E_OK;
    }

    FORCE_INLINE int write_buf(const char *buf, const uint32_t len) {
        return write_buf((const uint8_t *)buf, len);
    }
    FORCE_INLINE int read_buf(char *buf, const uint32_t want_len,
                              uint32_t &read_len) {
        return read_buf((uint8_t *)buf, want_len, read_len);
    }
    FORCE_INLINE int read_buf(char *buf, const int32_t want_len,
                              int32_t &read_len) {
        return read_buf((uint8_t *)buf, (uint32_t &)want_len,
                        (uint32_t &)read_len);
    }

    void purge_prev_pages(int purge_page_count = INT32_MAX) {
        Page *cur = head_.load();
        while (cur != tail_.load() && purge_page_count-- > 0) {
            Page *next = cur->next_.load();
            allocator_.free(cur);
            cur = next;
        }
        head_.store(cur);
    }

    /* ================ Part 3: writing internal buffers ================ */
    /*
     * use @acquire_buf function to get a buf, caller will fill the buf, and
     * after filling, caller use @buffer_used to notice ByteStream to update
     * internal variables.
     *
     * Note: it should be used in single thread.
     */
    struct Buffer {
        char *buf_;
        uint32_t len_;

        Buffer() : buf_(nullptr), len_(0) {}
    };

    Buffer acquire_buf() {
        Buffer b;
        if (common::E_OK != prepare_space()) {
            return b;
        }
        b.buf_ =
            (char *)(tail_.load()->buf_ + (total_size_.load() % page_size_));
        b.len_ = page_size_ - (total_size_.load() % page_size_);
        return b;
    }

    void buffer_used(uint32_t used_bytes) {
        ASSERT(used_bytes >= 1);
        // would not span page
        ASSERT((total_size_.load() / page_size_) ==
               ((total_size_.load() + used_bytes - 1) / page_size_));
        total_size_.atomic_aaf(used_bytes);
    }

    /* ================ Part 4: reading internal buffers ================ */
    /*
     * one-shot reader iterator
     * Do not use it if the host_ is still writing
     */
    struct BufferIterator {
        const ByteStream &host_;
        Page *cur_;
        Page *end_;
        int64_t total_size_;
        BufferIterator(const ByteStream &bs) : host_(bs) {
            cur_ = bs.head_.load();
            end_ = bs.tail_.load();
            total_size_ = bs.total_size_.load();
        }

        Buffer get_next_buf() {
            Buffer b;
            if (cur_ != nullptr) {
                b.buf_ = (char *)cur_->buf_;
                if (cur_ == end_ &&
                    host_.total_size_.load() % host_.page_size_ != 0) {
                    b.len_ = host_.total_size_.load() % host_.page_size_;
                } else {
                    b.len_ = host_.page_size_;
                }
                ASSERT(b.len_ > 0);
                cur_ = cur_->next_.load();
            }
            return b;
        }
    };
    BufferIterator init_buffer_iterator() const {
        return BufferIterator(*this);
    }

    /*
     * producer-consumer mode reader iterator
     * Note:
     * the host ByteStream should be configured as atomically.
     */
    struct Consumer {
        const ByteStream &host_;
        Page *cur_;
        uint32_t read_offset_within_cur_page_;
        int64_t total_end_offset_;  // for DEBUG

        Consumer(const ByteStream &bs) : host_(bs) {
            ASSERT(bs.head_.enable_atomic());
            cur_ = nullptr;
            read_offset_within_cur_page_ = 0;
            total_end_offset_ = 0;
        }

        Buffer get_next_buf(ByteStream &host) {
            Buffer b;
            if (UNLIKELY(host.head_.load() == nullptr)) {
                // host is empty, return empty buffer.
                return b;
            }
            if (UNLIKELY(cur_ == nullptr)) {
                // this consumer did not initialiazed.
                cur_ = host_.head_.load();
                read_offset_within_cur_page_ = 0;
            }

            // get tail position <tail_, total_size_> atomically
            Page *host_end = nullptr;
            uint32_t host_total_size = 0;
            while (true) {
                host_end = host_.tail_.load();
                host_total_size = host_.total_size_.load();
                if (host_end == host_.tail_.load()) {
                    break;
                }
            }

            while (true) {
                if (cur_ == host_end) {
                    if (host_total_size % host_.page_size_ == 0) {
                        if (read_offset_within_cur_page_ == host_.page_size_) {
                            return b;
                        } else {
                            b.buf_ = ((char *)(cur_->buf_)) +
                                     read_offset_within_cur_page_;
                            b.len_ =
                                host_.page_size_ - read_offset_within_cur_page_;
                            read_offset_within_cur_page_ = host_.page_size_;
                            total_end_offset_ += b.len_;
                            return b;
                        }
                    } else {
                        if (read_offset_within_cur_page_ ==
                            (host_total_size % host_.page_size_)) {
                            return b;
                        } else {
                            b.buf_ = ((char *)(cur_->buf_)) +
                                     read_offset_within_cur_page_;
                            b.len_ = (host_total_size % host_.page_size_) -
                                     read_offset_within_cur_page_;
                            read_offset_within_cur_page_ =
                                (host_total_size % host_.page_size_);
                            total_end_offset_ += b.len_;
                            return b;
                        }
                    }
                } else {
                    if (read_offset_within_cur_page_ == host_.page_size_) {
                        cur_ = cur_->next_.load();
                        read_offset_within_cur_page_ = 0;
                    } else {
                        b.buf_ = ((char *)(cur_->buf_)) +
                                 read_offset_within_cur_page_;
                        b.len_ =
                            host_.page_size_ - read_offset_within_cur_page_;
                        cur_ = cur_->next_.load();
                        read_offset_within_cur_page_ = 0;
                        total_end_offset_ += b.len_;
                        return b;
                    }
                }
            }
            ASSERT(b.len_ < (1 << 30));
            return b;
        }
    };

   private:
    FORCE_INLINE int prepare_space() {
        int ret = common::E_OK;
        if (UNLIKELY(tail_.load() == nullptr ||
                     total_size_.load() % page_size_ == 0)) {
            Page *p = nullptr;
            if (RET_FAIL(alloc_page(p))) {
                return ret;
            }
        } else {
            // do nothing
        }
        return ret;
    }

    FORCE_INLINE int check_space() {
        if (UNLIKELY(read_pos_ >= total_size_.load())) {
            return common::E_OUT_OF_RANGE;
        }
        if (UNLIKELY(read_page_ == nullptr)) {
            read_page_ = head_.load();
        } else if (UNLIKELY(read_pos_ % page_size_ == 0)) {
            read_page_ = read_page_->next_.load();
        }
        if (UNLIKELY(read_page_ == nullptr)) {
            return common::E_OUT_OF_RANGE;
        }
        return common::E_OK;
    }

    FORCE_INLINE int alloc_page(Page *&p) {
        int ret = common::E_OK;
        char *buf = (char *)allocator_.alloc(page_size_ + sizeof(Page), mid_);
        if (UNLIKELY(buf == nullptr)) {
            ret = common::E_OOM;
        } else {
            p = new (buf) Page(head_.enable_atomic());
            p->next_.store(nullptr);
            if (head_.load()) {
                tail_.load()->next_.store(p);
                tail_.store(p);
            } else {
                head_.store(p);
                tail_.store(p);
            }
        }
        // printf("\nByteStream alloc_page, this=%p, new_page=%p\n", this, p);
        return ret;
    }

    DISALLOW_COPY_AND_ASSIGN(ByteStream);

   private:
    BaseAllocator &allocator_;
    OptionalAtomic<Page *> head_;
    OptionalAtomic<Page *> tail_;
    Page *read_page_;  // only one thread is allow to reader this ByteStream
    OptionalAtomic<uint32_t> total_size_;  // total size in byte
    uint32_t read_pos_;                    // current reader position
    uint32_t marked_read_pos_;             // current reader position
    uint32_t page_size_;
    AllocModID mid_;
    Page wrapped_page_;
};

FORCE_INLINE int merge_byte_stream(ByteStream &sea, ByteStream &river,
                                   bool purge_river = false) {
    int ret = common::E_OK;
    ByteStream::BufferIterator buf_iter = river.init_buffer_iterator();
    while (true) {
        ByteStream::Buffer buf = buf_iter.get_next_buf();
        if (buf.buf_ == nullptr) {
            break;
        } else {
            if (RET_FAIL(sea.write_buf(buf.buf_, buf.len_))) {
                break;
            }
        }
        if (purge_river) {
            river.purge_prev_pages(1);
        }
    }
    return ret;
}

FORCE_INLINE int copy_bs_to_buf(ByteStream &bs, char *src_buf,
                                          uint32_t src_buf_len) {
    ByteStream::BufferIterator buf_iter = bs.init_buffer_iterator();
    uint32_t copyed_len = 0;
    while (true) {
        ByteStream::Buffer buf = buf_iter.get_next_buf();
        if (buf.buf_ == nullptr) {
            break;
        } else {
            if (src_buf_len - copyed_len < buf.len_) {
                return E_BUF_NOT_ENOUGH;
            }
            memcpy(src_buf + copyed_len, buf.buf_, buf.len_);
            copyed_len += buf.len_;
        }
    }
    return E_OK;
}

FORCE_INLINE uint32_t get_var_uint_size(
    uint32_t
        ui32)  // return: the length of usigned number after varint encoding.
{
    uint32_t bytes = 0;
    while ((ui32 & 0xFFFFFF80) != 0) {
        bytes++;
        ui32 = ui32 >> 7;
    }
    return ++bytes;
}

#if DEBUG_SE
FORCE_INLINE void DEBUG_print_byte_stream(const char *print_tag,
                                          ByteStream &b) {
    const int32_t WRAP_COUNT = 16;
    int32_t print_char_count = 0;
    ByteStream::BufferIterator buf_iter = b.init_buffer_iterator();
    while (true) {
        ByteStream::Buffer buf = buf_iter.get_next_buf();
        if (buf.buf_ == nullptr) {
            break;
        } else {
            // print_hex(buf.buf_, buf.len_);
            printf("\n %s: buf.buf_=%p, buf.len_=%u\n", print_tag, buf.buf_,
                   buf.len_);
            for (uint32_t i = 0; i < buf.len_; i++) {
                if (print_char_count++ % WRAP_COUNT == 0) {
                    printf("\n %s offset=0x%05x :  ", print_tag,
                           print_char_count - 1);
                }
                printf("%02x ", (uint8_t)buf.buf_[i]);
            }
        }
    }
    printf("\n\n");
}

FORCE_INLINE void DEBUG_hex_dump_buf(const char *print_tag, const char *buf,
                                     int32_t len) {
    const int32_t WRAP_COUNT = 16;
    for (int i = 0; i < len; i++) {
        if (i % WRAP_COUNT == 0) {
            printf("\n%s", print_tag);
        }
        printf("%02x ", (uint8_t)(buf[i]));
    }
    printf("\n");
}
#endif  // ifndef NDEBUG

class SerializationUtil {
   public:
    FORCE_INLINE static int write_ui8(uint8_t ui8, ByteStream &out) {
        return out.write_buf(&ui8, 1);
    }
    FORCE_INLINE static int write_ui16(uint16_t ui16, ByteStream &out) {
        uint8_t buf[2];
        buf[0] = (uint8_t)((ui16 >> 8) & 0xFF);
        buf[1] = (uint8_t)((ui16)&0xFF);
        return out.write_buf(buf, 2);
    }
    FORCE_INLINE static int write_ui32(uint32_t ui32, ByteStream &out) {
        uint8_t buf[4];
        buf[0] = (uint8_t)((ui32 >> 24) & 0xFF);
        buf[1] = (uint8_t)((ui32 >> 16) & 0xFF);
        buf[2] = (uint8_t)((ui32 >> 8) & 0xFF);
        buf[3] = (uint8_t)((ui32)&0xFF);
        return out.write_buf(buf, 4);
    }
    FORCE_INLINE static int write_ui64(uint64_t ui64, ByteStream &out) {
        // big-endian: most signification byte at smaller address
        // refer to tsfile.utils.BytesUtil
        uint8_t buf[8];
        buf[0] = (uint8_t)((ui64 >> 56) & 0xFF);
        buf[1] = (uint8_t)((ui64 >> 48) & 0xFF);
        buf[2] = (uint8_t)((ui64 >> 40) & 0xFF);
        buf[3] = (uint8_t)((ui64 >> 32) & 0xFF);
        buf[4] = (uint8_t)((ui64 >> 24) & 0xFF);
        buf[5] = (uint8_t)((ui64 >> 16) & 0xFF);
        buf[6] = (uint8_t)((ui64 >> 8) & 0xFF);
        buf[7] = (uint8_t)((ui64)&0xFF);
        return out.write_buf(buf, 8);
    }

    FORCE_INLINE static int read_ui8(uint8_t &ui8, ByteStream &in) {
        int ret = common::E_OK;
        char buf[1];
        uint32_t read_len = 0;
        ret = in.read_buf(buf, 1, read_len);
        ui8 = (uint8_t)buf[0];
        return ret;
    }
    FORCE_INLINE static int read_ui16(uint16_t &ui16, ByteStream &in) {
        int ret = common::E_OK;
        uint8_t buf[2];
        uint32_t read_len = 0;
        if (RET_FAIL(in.read_buf(buf, 2, read_len))) {
            return ret;
        }
        ui16 = buf[0];
        ui16 = (ui16 << 8) | buf[1];
        return ret;
    }
    FORCE_INLINE static int read_ui32(uint32_t &ui32, ByteStream &in) {
        int ret = common::E_OK;
        uint8_t buf[4];
        uint32_t read_len = 0;
        if (RET_FAIL(in.read_buf(buf, 4, read_len))) {
            return ret;
        }
        ui32 = buf[0];
        ui32 = (ui32 << 8) | (buf[1] & 0xFF);
        ui32 = (ui32 << 8) | (buf[2] & 0xFF);
        ui32 = (ui32 << 8) | (buf[3] & 0xFF);
        return ret;
    }
    FORCE_INLINE static int read_ui64(uint64_t &ui64, ByteStream &in) {
        int ret = common::E_OK;
        uint8_t buf[8];
        uint32_t read_len = 0;
        if (RET_FAIL(in.read_buf(buf, 8, read_len))) {
            return ret;
        }
        ui64 = buf[0];
        ui64 = (ui64 << 8) | (buf[1] & 0xFF);
        ui64 = (ui64 << 8) | (buf[2] & 0xFF);
        ui64 = (ui64 << 8) | (buf[3] & 0xFF);
        ui64 = (ui64 << 8) | (buf[4] & 0xFF);
        ui64 = (ui64 << 8) | (buf[5] & 0xFF);
        ui64 = (ui64 << 8) | (buf[6] & 0xFF);
        ui64 = (ui64 << 8) | (buf[7] & 0xFF);
        return ret;
    }
    // caller guarantee buffer has at least 1 byte
    FORCE_INLINE static uint8_t read_ui8(char *buffer) {
        return *(uint8_t *)buffer;
    }

    // caller guarantee buffer has at least 2 bytes
    FORCE_INLINE static uint16_t read_ui16(char *buffer) {
        uint8_t *buf = (uint8_t *)buffer;
        uint16_t ui16 = buf[0];
        ui16 = (ui16 << 8) | (buf[1] & 0xFF);
        return ui16;
    }
    // caller guarantee buffer has at least 4 bytes
    FORCE_INLINE static uint32_t read_ui32(char *buffer) {
        uint8_t *buf = (uint8_t *)buffer;
        uint32_t ui32 = buf[0];
        ui32 = (ui32 << 8) | (buf[1] & 0xFF);
        ui32 = (ui32 << 8) | (buf[2] & 0xFF);
        ui32 = (ui32 << 8) | (buf[3] & 0xFF);
        return ui32;
    }
    // caller guarantee buffer has at least 8 bytes
    FORCE_INLINE static uint64_t read_ui64(char *buffer) {
        uint8_t *buf = (uint8_t *)buffer;
        uint64_t ui64 = buf[0];
        ui64 = (ui64 << 8) | (buf[1] & 0xFF);
        ui64 = (ui64 << 8) | (buf[2] & 0xFF);
        ui64 = (ui64 << 8) | (buf[3] & 0xFF);
        ui64 = (ui64 << 8) | (buf[4] & 0xFF);
        ui64 = (ui64 << 8) | (buf[5] & 0xFF);
        ui64 = (ui64 << 8) | (buf[6] & 0xFF);
        ui64 = (ui64 << 8) | (buf[7] & 0xFF);
        return ui64;
    }

    FORCE_INLINE static int write_float(float f, ByteStream &out) {
        uint8_t bytes[4];
        float_to_bytes(f, bytes);
        return out.write_buf(bytes, 4);
    }
    FORCE_INLINE static int read_float(float &f, ByteStream &in) {
        int ret = common::E_OK;
        uint8_t bytes[4];
        uint32_t read_len = 0;
        if (RET_FAIL(in.read_buf(bytes, 4, read_len))) {
        } else if (read_len != 4) {
            ret = common::E_BUF_NOT_ENOUGH;
        } else {
            f = bytes_to_float(bytes);
        }
        return ret;
    }
    FORCE_INLINE static float read_float(char *buffer) {
        uint8_t *buf = (uint8_t *)buffer;
        return bytes_to_float(buf);
    }
    FORCE_INLINE static int write_double(double d, ByteStream &out) {
        uint8_t bytes[8];
        double_to_bytes(d, bytes);
        return out.write_buf(bytes, 8);
    }
    FORCE_INLINE static int read_double(double &d, ByteStream &in) {
        int ret = common::E_OK;
        uint32_t read_len = 0;
        uint8_t bytes[8];
        if (RET_FAIL(in.read_buf(bytes, 8, read_len))) {
        } else if (read_len != 8) {
            ret = common::E_BUF_NOT_ENOUGH;
        } else {
            d = bytes_to_double(bytes);
        }
        return ret;
    }
    FORCE_INLINE static double read_double(char *buffer) {
        uint8_t *buf = (uint8_t *)buffer;
        return bytes_to_double(buf);
    }

    FORCE_INLINE static int write_i8(int8_t i8, ByteStream &out) {
        return write_ui8((uint8_t)i8, out);
    }
    FORCE_INLINE static int write_i16(int16_t i16, ByteStream &out) {
        return write_ui16((uint16_t)i16, out);
    }
    FORCE_INLINE static int write_i32(int32_t i32, ByteStream &out) {
        return write_ui32((uint32_t)i32, out);
    }
    FORCE_INLINE static int write_i64(int64_t i64, ByteStream &out) {
        return write_ui64((uint64_t)i64, out);
    }

    FORCE_INLINE static int read_i8(int8_t &i8, ByteStream &in) {
        return read_ui8((uint8_t &)i8, in);
    }
    FORCE_INLINE static int read_i16(int16_t &i16, ByteStream &in) {
        return read_ui16((uint16_t &)i16, in);
    }
    FORCE_INLINE static int read_i32(int32_t &i32, ByteStream &in) {
        return read_ui32((uint32_t &)i32, in);
    }
    FORCE_INLINE static int read_i64(int64_t &i64, ByteStream &in) {
        return read_ui64((uint64_t &)i64, in);
    }

    // TODO more test on var_xxx
    FORCE_INLINE static int do_write_var_uint(uint32_t ui32, ByteStream &out) {
        int ret = common::E_OK;
        while ((ui32 & 0xFFFFFF80) != 0) {
            if (RET_FAIL(write_ui8((ui32 & 0x7F) | 0x80, out))) {
                return ret;
            }
            ui32 = ui32 >> 7;
        }
        return write_ui8(ui32 & 0x7F, out);
    }
    FORCE_INLINE static int do_write_var_uint(uint32_t ui32, char *out_buf,
                                              const uint32_t out_buf_len) {
        uint32_t offset = 0;
        while ((ui32 & 0xFFFFFF80) != 0) {
            if (offset >= out_buf_len) {
                return common::E_BUF_NOT_ENOUGH;
            }
            *(out_buf + offset) = (ui32 & 0x7F) | 0x80;
            ui32 = ui32 >> 7;
            offset++;
        }
        if (offset >= out_buf_len) {
            return common::E_BUF_NOT_ENOUGH;
        }
        *(out_buf + offset) = (ui32 & 0x7F);
        return common::E_OK;
    }
    FORCE_INLINE static int do_read_var_uint(uint32_t &ui32, ByteStream &in) {
        // Follow readUnsignedVarInt in ReadWriteForEncodingUtils.java
        int ret = common::E_OK;
        ui32 = 0;
        int i = 0;
        uint8_t ui8 = 0;
        if (RET_FAIL(read_ui8(ui8, in))) {
            return ret;
        }
        while (ui8 != 0xF && (ui8 & 0x80) != 0) {
            ui32 = ui32 | ((ui8 & 0x7F) << i);
            i = i + 7;
            if (RET_FAIL(read_ui8(ui8, in))) {
                return ret;
            }
        }
        ui32 = ui32 | (ui8 << i);
        return ret;
    }
    FORCE_INLINE static int do_read_var_uint(uint32_t &ui32, char *in_buf,
                                             int in_buf_len, int *ret_offset) {
        ui32 = 0;
        int i = 0;
        uint8_t ui8 = 0;
        int offset = 0;
        if (offset < in_buf_len) {
            ui8 = *(uint8_t *)(in_buf + offset);
            offset++;
        } else {
            return common::E_BUF_NOT_ENOUGH;
        }
        while (ui8 != 0xF && (ui8 & 0x80) != 0) {
            ui32 = ui32 | ((ui8 & 0x7F) << i);
            i = i + 7;
            if (offset < in_buf_len) {
                ui8 = *(uint8_t *)(in_buf + offset);
                offset++;
            } else {
                return common::E_BUF_NOT_ENOUGH;
            }
        }
        ui32 = ui32 | (ui8 << i);
        if (ret_offset != nullptr) {
            *ret_offset = offset;
        }
        return common::E_OK;
    }
    FORCE_INLINE static int write_var_int(int32_t i32, ByteStream &out) {
        // TODO 8byte to 4byte.
        // but in IoTDB java, it has only write_var_uint(i32)
        i32 = i32 << 1;
        return do_write_var_uint((uint32_t)i32, out);
    }
    FORCE_INLINE static int read_var_int(int32_t &i32, ByteStream &in) {
        int ret = common::E_OK;
        uint32_t ui32;
        if (RET_FAIL(do_read_var_uint(ui32, in))) {
        } else {
            i32 = (int32_t)(ui32 >> 1);
        }
        return ret;
    }
    FORCE_INLINE static int write_var_uint(uint32_t ui32, ByteStream &out) {
        return do_write_var_uint(ui32, out);
    }
    FORCE_INLINE static int write_var_uint(uint32_t ui32, char *out_buf,
                                           const uint32_t out_buf_len) {
        return do_write_var_uint(ui32, out_buf, out_buf_len);
    }
    FORCE_INLINE static int read_var_uint(uint32_t &ui32, ByteStream &in) {
        return do_read_var_uint(ui32, in);
    }
    FORCE_INLINE static int read_var_uint(uint32_t &ui32, char *in_buf,
                                          int in_buf_len,
                                          int *ret_offset = nullptr) {
        return do_read_var_uint(ui32, in_buf, in_buf_len, ret_offset);
    }

    FORCE_INLINE static int write_str(const std::string &str, ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(write_var_int(((int32_t)str.size()), out))) {
        } else if (RET_FAIL(out.write_buf(str.c_str(), str.size()))) {
        }
        return ret;
    }
    FORCE_INLINE static int read_str(std::string &str, ByteStream &in) {
        int ret = common::E_OK;
        int32_t len = 0;
        int32_t read_len = 0;
        if (RET_FAIL(read_var_int(len, in))) {
        } else {
            char *tmp_buf = (char *)malloc(len + 1);
            tmp_buf[len] = '\0';
            if (RET_FAIL(in.read_buf(tmp_buf, len, read_len))) {
            } else if (len != read_len) {
                ret = E_BUF_NOT_ENOUGH;
            } else {
                str = std::string(tmp_buf);
            }
            free(tmp_buf);
        }
        return ret;
    }
    FORCE_INLINE static int write_mystring(const String &str, ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(write_var_int(str.len_, out))) {
        } else if (RET_FAIL(out.write_buf(str.buf_, str.len_))) {
        }
        return ret;
    }
    FORCE_INLINE static int read_mystring(String &str, common::PageArena *pa,
                                          ByteStream &in) {
        int ret = common::E_OK;
        int32_t len = 0;
        int32_t read_len = 0;
        if (RET_FAIL(read_var_int(len, in))) {
        } else {
            char *buf = (char *)pa->alloc(len);
            if (IS_NULL(buf)) {
                ret = common::E_OOM;
            } else {
                if (RET_FAIL(in.read_buf(buf, len, read_len))) {
                } else if (len != read_len) {
                    ret = E_BUF_NOT_ENOUGH;
                } else {
                    str.buf_ = buf;
                    str.len_ = len;
                }
            }
        }
        return ret;
    }
    FORCE_INLINE static int write_char(char ch, ByteStream &out) {
        return write_ui8(ch, out);
    }
    FORCE_INLINE static int read_char(char &ch, ByteStream &in) {
        return read_ui8((uint8_t &)ch, in);
    }
};

FORCE_INLINE bool deserialize_buf_not_enough(int ret) {
    return ret == E_OUT_OF_RANGE || ret == E_PARTIAL_READ;
}
FORCE_INLINE char *get_bytes_from_bytestream(ByteStream &bs) {
    if (bs.total_size() == 0) {
        return nullptr;
    }
    uint32_t size = bs.total_size();
    char *ret_buf = (char *)malloc(size);
    if (ret_buf == nullptr) {
        return nullptr;
    }

    ByteStream::BufferIterator buf_iter = bs.init_buffer_iterator();
    uint32_t offset = 0;
    while (true) {
        ByteStream::Buffer buf = buf_iter.get_next_buf();
        if (buf.buf_ == nullptr) {
            break;
        } else {
            assert(offset + buf.len_ <= size);
            memcpy(ret_buf + offset, buf.buf_, buf.len_);
            offset += buf.len_;
        }
    }
    assert(offset == size);
    return ret_buf;
}

}  // end namespace common
#endif  // COMMON_ALLOCATOR_BYTE_STREAM_H
