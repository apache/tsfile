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

#ifndef COMMON_TSFILE_COMMON_H
#define COMMON_TSFILE_COMMON_H

#include <cstring>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/allocator/my_string.h"
#include "common/allocator/page_arena.h"
#include "common/config/config.h"
#include "common/container/list.h"
#include "device_id.h"
#include "reader/bloom_filter.h"
#include "statistic.h"
#include "utils/db_utils.h"
#include "utils/storage_utils.h"

namespace storage {

extern const char *MAGIC_STRING_TSFILE;
constexpr int MAGIC_STRING_TSFILE_LEN = 6;
extern const char VERSION_NUM_BYTE;
extern const char CHUNK_GROUP_HEADER_MARKER;
extern const char CHUNK_HEADER_MARKER;
extern const char ONLY_ONE_PAGE_CHUNK_HEADER_MARKER;
extern const char SEPARATOR_MARKER;
extern const char OPERATION_INDEX_RANGE;

typedef int64_t TsFileID;

// TODO review the String.len_ used

// Note, in tsfile_io_writer, we just writer fields of PageHeader
// instead of do a serialize of PageHeader object.
// That is because statistic_ in PageHeader may be omitted if only
// one page exists in the chunk but we know that fact after we writer
// the first page.
struct PageHeader {
    uint32_t uncompressed_size_;
    uint32_t compressed_size_;
    Statistic *statistic_;

    PageHeader()
        : uncompressed_size_(0), compressed_size_(0), statistic_(nullptr) {}
    ~PageHeader() { reset(); }
    void reset() {
        if (statistic_ != nullptr) {
            StatisticFactory::free(statistic_);
            statistic_ = nullptr;
        }
        uncompressed_size_ = 0;
        compressed_size_ = 0;
    }
    int deserialize_from(common::ByteStream &in, bool deserialize_stat,
                         common::TSDataType data_type) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_var_uint(
                uncompressed_size_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_var_uint(
                       compressed_size_, in))) {
        } else if (deserialize_stat) {
            statistic_ = StatisticFactory::alloc_statistic(data_type);
            if (IS_NULL(statistic_)) {
                return common::E_OOM;
            } else if (RET_FAIL(statistic_->deserialize_from(in))) {
            }
        }
        return ret;
    }

    /** max page header size without statistics. */
    static int estimat_max_page_header_size_without_statistics() {
        // uncompressedSize, compressedSize
        // because we use unsigned varInt to encode these two integer, each
        // unsigned varInt will cost at most 5 bytes
        return 2 * (4 + 1);
    }

#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os, const PageHeader &h) {
        os << "{uncompressed_size_=" << h.uncompressed_size_
           << ", compressed_size_=" << h.uncompressed_size_;
        if (h.statistic_ == nullptr) {
            os << ", stat=nil}";
        } else {
            os << ", stat=" << h.statistic_->to_string() << "}";
        }
        return os;
    }
#endif
};

struct ChunkHeader {
    ChunkHeader()
        : measurement_name_(""),
          data_size_(0),
          data_type_(common::INVALID_DATATYPE),
          compression_type_(common::INVALID_COMPRESSION),
          encoding_type_(common::INVALID_ENCODING),
          num_of_pages_(0),
          serialized_size_(0),
          chunk_type_(0) {}

    void reset() {
        data_size_ = 0;
        num_of_pages_ = 0;
        serialized_size_ = 0;
        chunk_type_ = 0;
    }

    ~ChunkHeader() = default;

    int serialize_to(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_char(chunk_type_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_var_str(
                       measurement_name_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_var_uint(
                       data_size_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_char(data_type_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_char(
                       compression_type_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_char(
                       encoding_type_, out))) {
        }
        return ret;
    }
    int deserialize_from(common::ByteStream &in) {
        int ret = common::E_OK;
        in.mark_read_pos();
        if (RET_FAIL(common::SerializationUtil::read_char(chunk_type_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_var_str(
                       measurement_name_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_var_uint(data_size_,
                                                                     in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_char(
                       (char &)data_type_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_char(
                       (char &)compression_type_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_char(
                       (char &)encoding_type_, in))) {
        } else {
            serialized_size_ = in.get_mark_len();
        }
        return ret;
    }
#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os, const ChunkHeader &h) {
        os << "{measurement_name=" << h.measurement_name_
           << ", data_size=" << h.data_size_ << ", data_type=" << h.data_type_
           << ", compression_type=" << h.compression_type_
           << ", encoding_type=" << h.encoding_type_
           << ", num_of_pages=" << h.num_of_pages_
           << ", serialized_size=" << h.serialized_size_
           << ", chunk_type=" << (int)h.chunk_type_ << "}";
        return os;
    }
#endif

    std::string measurement_name_;
    uint32_t data_size_;
    common::TSDataType data_type_;
    common::CompressionType compression_type_;
    common::TSEncoding encoding_type_;
    int32_t num_of_pages_;
    int32_t serialized_size_;  // TODO seems no usage
    char chunk_type_;          // TODO give a description here

    static const int MIN_SERIALIZED_SIZE = 7;
};

struct ChunkMeta {
    // std::string measurement_name_;
    common::String measurement_name_;
    common::TSDataType data_type_;
    int64_t offset_of_chunk_header_;
    Statistic *statistic_;
    char mask_;
    common::TSEncoding encoding_;
    common::CompressionType compression_type_;

    ChunkMeta()
        : measurement_name_(),
          data_type_(),
          offset_of_chunk_header_(0),
          statistic_(nullptr),
          mask_(0) {}

    int init(const common::String &measurement_name,
             common::TSDataType data_type, int64_t offset_of_chunk_header,
             Statistic *stat, char mask, common::TSEncoding encoding,
             common::CompressionType compression_type, common::PageArena &pa) {
        // TODO check parameter valid
        measurement_name_.dup_from(measurement_name, pa);
        data_type_ = data_type;
        offset_of_chunk_header_ = offset_of_chunk_header;
        statistic_ = stat;
        mask_ = mask;
        encoding_ = encoding;
        compression_type_ = compression_type;
        return common::E_OK;
    }
    FORCE_INLINE void clone_statistic_from(Statistic *stat) {
        clone_statistic(stat, statistic_, data_type_);
    }
    FORCE_INLINE int clone_from(ChunkMeta &that, common::PageArena *pa) {
        int ret = common::E_OK;
        if (RET_FAIL(measurement_name_.dup_from(that.measurement_name_, *pa))) {
            return ret;
        }
        data_type_ = that.data_type_;
        offset_of_chunk_header_ = that.offset_of_chunk_header_;
        if (that.statistic_ != nullptr) {
            statistic_ =
                StatisticFactory::alloc_statistic_with_pa(data_type_, pa);
            if (IS_NULL(statistic_)) {
                return common::E_OOM;
            }
            clone_statistic_from(that.statistic_);
        }
        mask_ = that.mask_;
        return ret;
    }
    int serialize_to(common::ByteStream &out, bool serialize_statistic) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_i64(
                offset_of_chunk_header_, out))) {
        } else if (serialize_statistic) {
            ret = statistic_->serialize_to(out);
        }
        return ret;
    }
    int deserialize_from(common::ByteStream &in, bool deserialize_stat,
                         common::PageArena *pa) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_i64(
                offset_of_chunk_header_, in))) {
        } else if (deserialize_stat) {
            statistic_ =
                StatisticFactory::alloc_statistic_with_pa(data_type_, pa);
            if (IS_NULL(statistic_)) {
                ret = common::E_OOM;
            } else {
                ret = statistic_->deserialize_from(in);
            }
        }
        return ret;
    }
#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os, const ChunkMeta &cm) {
        os << "{measurement_name=" << cm.measurement_name_
           << ", data_type=" << cm.data_type_
           << ", offset_of_chunk_header=" << cm.offset_of_chunk_header_
           << ", mask=" << ((int)cm.mask_);
        if (cm.statistic_ == nullptr) {
            os << ", statistic=nil}";
        } else {
            os << ", statistic=" << cm.statistic_->to_string() << "}";
        }
        return os;
    }
#endif
};

struct ChunkGroupMeta {
    std::shared_ptr<IDeviceID> device_id_;
    common::SimpleList<ChunkMeta *> chunk_meta_list_;

    explicit ChunkGroupMeta(common::PageArena *pa_ptr)
        : chunk_meta_list_(pa_ptr) {}

    FORCE_INLINE int init(std::shared_ptr<IDeviceID> device_id) {
        device_id_ = device_id;
        return 0;
    }
    FORCE_INLINE int push(ChunkMeta *cm) {
        return chunk_meta_list_.push_back(cm);
    }
};

class ITimeseriesIndex {
   public:
    ITimeseriesIndex() {}
    ~ITimeseriesIndex() {}
    virtual common::SimpleList<ChunkMeta *> *get_chunk_meta_list() const {
        return nullptr;
    }
    virtual common::SimpleList<ChunkMeta *> *get_time_chunk_meta_list() const {
        return nullptr;
    }
    virtual common::SimpleList<ChunkMeta *> *get_value_chunk_meta_list() const {
        return nullptr;
    }

    virtual common::String get_measurement_name() const {
        return common::String();
    }
    virtual common::TSDataType get_data_type() const {
        return common::INVALID_DATATYPE;
    }
    virtual Statistic *get_statistic() const { return nullptr; }
};

/*
 * A TimeseriesIndex may have one or more chunk metas,
 * that means we have such a map: <Timeseries, List<ChunkMeta>>.
 */
class TimeseriesIndex : public ITimeseriesIndex {
   public:
    static const uint32_t CHUNK_META_LIST_SERIALIZED_BUF_PAGE_SIZE = 128;
    static const uint32_t PAGE_ARENA_PAGE_SIZE = 256;
    static const common::AllocModID PAGE_ARENA_MOD_ID =
        common::MOD_TIMESERIES_INDEX_OBJ;

   public:
    TimeseriesIndex()
        : timeseries_meta_type_((char)255),
          chunk_meta_list_data_size_(0),
          measurement_name_(),
          ts_id_(),
          data_type_(common::INVALID_DATATYPE),
          statistic_(nullptr),
          statistic_from_pa_(false),
          chunk_meta_list_serialized_buf_(
              CHUNK_META_LIST_SERIALIZED_BUF_PAGE_SIZE, PAGE_ARENA_MOD_ID),
          chunk_meta_list_(nullptr) {
        // page_arena_.init(PAGE_ARENA_PAGE_SIZE, PAGE_ARENA_MOD_ID);
    }
    ~TimeseriesIndex() { destroy(); }
    void destroy() {
        // page_arena_.destroy();
        reset();
    }
    void reset()  // FIXME reuse
    {
        timeseries_meta_type_ = 0;
        chunk_meta_list_data_size_ = 0;
        measurement_name_.reset();
        ts_id_.reset();
        data_type_ = common::VECTOR;
        chunk_meta_list_serialized_buf_.reset();
        if (statistic_ != nullptr && !statistic_from_pa_) {
            StatisticFactory::free(statistic_);
            statistic_ = nullptr;
        }
    }

    int add_chunk_meta(ChunkMeta *chunk_meta, bool serialize_statistic);
    FORCE_INLINE int set_measurement_name(common::String &measurement_name,
                                          common::PageArena &pa) {
        return measurement_name_.dup_from(measurement_name, pa);
    }
    FORCE_INLINE void set_measurement_name(common::String &measurement_name) {
        measurement_name_.shallow_copy_from(measurement_name);
    }
    FORCE_INLINE virtual common::String get_measurement_name() const {
        return measurement_name_;
    }
    virtual inline common::SimpleList<ChunkMeta *> *get_chunk_meta_list()
        const {
        return chunk_meta_list_;
    }
    FORCE_INLINE void set_ts_meta_type(char ts_meta_type) {
        timeseries_meta_type_ = ts_meta_type;
    }
    FORCE_INLINE void set_data_type(common::TSDataType data_type) {
        data_type_ = data_type;
    }
    FORCE_INLINE virtual common::TSDataType get_data_type() const {
        return data_type_;
    }
    int init_statistic(common::TSDataType data_type) {
        if (statistic_ != nullptr &&
            !statistic_from_pa_) {  // clear old statistic
            StatisticFactory::free(statistic_);
            statistic_ = nullptr;
        }
        statistic_ = StatisticFactory::alloc_statistic(data_type);
        if (IS_NULL(statistic_)) {
            return common::E_OOM;
        }
        statistic_->reset();
        return common::E_OK;
    }
    virtual Statistic *get_statistic() const { return statistic_; }
    common::TsID get_ts_id() const { return ts_id_; }

    FORCE_INLINE void finish() {
        chunk_meta_list_data_size_ =
            chunk_meta_list_serialized_buf_.total_size();
    }

    int serialize_to(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_char(
                timeseries_meta_type_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_mystring(
                       measurement_name_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_char(data_type_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_var_uint(
                       chunk_meta_list_data_size_, out))) {
        } else if (RET_FAIL(statistic_->serialize_to(out))) {
        } else if (RET_FAIL(merge_byte_stream(
                       out, chunk_meta_list_serialized_buf_))) {
        }
        return ret;
    }

    int deserialize_from(common::ByteStream &in, common::PageArena *pa) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_char(timeseries_meta_type_,
                                                          in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_mystring(
                       measurement_name_, pa, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_char(
                       (char &)data_type_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_var_uint(
                       chunk_meta_list_data_size_, in))) {
        } else if (nullptr ==
                   (statistic_ = StatisticFactory::alloc_statistic_with_pa(
                        data_type_, pa))) {
            ret = common::E_OOM;
        } else if (RET_FAIL(statistic_->deserialize_from(in))) {
        } else {
            statistic_from_pa_ = true;
            void *chunk_meta_list_buf = pa->alloc(sizeof(*chunk_meta_list_));
            if (IS_NULL(chunk_meta_list_buf)) {
                return common::E_OOM;
            }
            const bool deserialize_chunk_meta_statistic =
                (timeseries_meta_type_ & 0x3F);  // TODO
            chunk_meta_list_ =
                new (chunk_meta_list_buf) common::SimpleList<ChunkMeta *>(pa);
            uint32_t start_pos = in.read_pos();
            while (IS_SUCC(ret) &&
                   in.read_pos() < start_pos + chunk_meta_list_data_size_) {
                void *cm_buf = pa->alloc(sizeof(ChunkMeta));
                if (IS_NULL(cm_buf)) {
                    ret = common::E_OOM;
                } else {
                    ChunkMeta *cm = new (cm_buf) ChunkMeta;
                    cm->measurement_name_.shallow_copy_from(
                        this->measurement_name_);
                    cm->data_type_ = this->data_type_;
                    cm->mask_ = 0;  // TODO
                    if (RET_FAIL(cm->deserialize_from(
                            in, deserialize_chunk_meta_statistic, pa))) {
                    } else if (RET_FAIL(chunk_meta_list_->push_back(cm))) {
                    }
                }
            }
        }
        return ret;
    }

    int clone_from(const TimeseriesIndex &that, common::PageArena *pa) {
        int ret = common::E_OK;
        timeseries_meta_type_ = that.timeseries_meta_type_;
        chunk_meta_list_data_size_ = that.chunk_meta_list_data_size_;
        ts_id_ = that.ts_id_;
        data_type_ = that.data_type_;

        statistic_ = StatisticFactory::alloc_statistic_with_pa(data_type_, pa);
        if (IS_NULL(statistic_)) {
            return common::E_OOM;
        }
        clone_statistic(that.statistic_, this->statistic_, data_type_);
        statistic_from_pa_ = true;

        if (RET_FAIL(measurement_name_.dup_from(that.measurement_name_, *pa))) {
            return ret;
        }

        if (that.chunk_meta_list_ != nullptr) {
            void *buf = pa->alloc(sizeof(*chunk_meta_list_));
            if (IS_NULL(buf)) {
                return common::E_OOM;
            }
            chunk_meta_list_ = new (buf) common::SimpleList<ChunkMeta *>(pa);
            common::SimpleList<ChunkMeta *>::Iterator it;
            for (it = that.chunk_meta_list_->begin();
                 IS_SUCC(ret) && it != that.chunk_meta_list_->end(); it++) {
                ChunkMeta *cm = it.get();
                void *cm_buf = pa->alloc(sizeof(ChunkMeta));
                if (IS_NULL(cm_buf)) {
                    return common::E_OOM;
                } else {
                    ChunkMeta *my_cm = new (cm_buf) ChunkMeta;
                    if (RET_FAIL(my_cm->clone_from(*cm, pa))) {
                    } else if (RET_FAIL(chunk_meta_list_->push_back(my_cm))) {
                    }
                }
            }
        }  // end (that.chunk_meta_list_ != nullptr)
        return ret;
    }
#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os,
                                    const TimeseriesIndex &tsi) {
        os << "{meta_type=" << (int)tsi.timeseries_meta_type_
           << ", chunk_meta_list_data_size=" << tsi.chunk_meta_list_data_size_
           << ", measurement_name=" << tsi.measurement_name_
           << ", ts_id=" << tsi.ts_id_.to_string()
           << ", data_type=" << common::get_data_type_name(tsi.data_type_)
           << ", statistic=" << tsi.statistic_->to_string();

        if (tsi.chunk_meta_list_) {
            os << ", chunk_meta_list={";
            int count = 0;
            common::SimpleList<ChunkMeta *>::Iterator it =
                tsi.chunk_meta_list_->begin();
            for (; it != tsi.chunk_meta_list_->end(); it++, count++) {
                if (count != 0) {
                    os << ", ";
                }
                os << "[" << count << "]={" << *it.get() << "}";
            }
            os << "}";
        }
        return os;
    }
#endif
   private:
    /*
     * If this timeseries has more than one chunk meta, timeseries_meta_type_
     * is 1. Otherwise timeseries_meta_type_ is 0. It also should OR with mask
     * of chunk meta.
     */
    char timeseries_meta_type_;

    // Sum of chunk meta serialized size in List<ChunkMeta> of this timeseries.
    uint32_t chunk_meta_list_data_size_;

    // std::string measurement_name_;
    common::String measurement_name_;
    common::TsID ts_id_;
    common::TSDataType data_type_;

    /*
     * If TimeseriesIndex has only one ChunkMeta, then
     * TimeseriesIndex.statistic_ is duplicated with ChunkMeta.statistic_. In
     * this case, we do not serialize ChunkMeta.statistic_.
     */
    Statistic *statistic_;
    bool statistic_from_pa_;
    common::ByteStream chunk_meta_list_serialized_buf_;
    // common::PageArena page_arena_;
    common::SimpleList<ChunkMeta *> *chunk_meta_list_;  // for deserialize_from
};

class AlignedTimeseriesIndex : public ITimeseriesIndex {
   public:
    TimeseriesIndex *time_ts_idx_;
    TimeseriesIndex *value_ts_idx_;

    AlignedTimeseriesIndex() {}
    ~AlignedTimeseriesIndex() {}
    virtual common::SimpleList<ChunkMeta *> *get_time_chunk_meta_list() const {
        return time_ts_idx_->get_chunk_meta_list();
    }
    virtual common::SimpleList<ChunkMeta *> *get_value_chunk_meta_list() const {
        return value_ts_idx_->get_chunk_meta_list();
    }

    virtual common::String get_measurement_name() const {
        return value_ts_idx_->get_measurement_name();
    }
    virtual common::TSDataType get_data_type() const {
        return time_ts_idx_->get_data_type();
    }
    virtual Statistic *get_statistic() const {
        return value_ts_idx_->get_statistic();
    }

#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os,
                                    const AlignedTimeseriesIndex &tsi) {
        os << "time_ts_idx=" << *tsi.time_ts_idx_;
        os << ", value_ts_idx=" << *tsi.value_ts_idx_;
        return os;
    }
#endif
};

class TSMIterator {
   public:
    explicit TSMIterator(
        common::SimpleList<ChunkGroupMeta *> &chunk_group_meta_list)
        : chunk_group_meta_list_(chunk_group_meta_list),
          chunk_group_meta_iter_(),
          chunk_meta_iter_() {}

    // sort => iterate
    int init();
    bool has_next() const;
    int get_next(std::shared_ptr<IDeviceID> &ret_device_id,
                 common::String &ret_measurement_name,
                 TimeseriesIndex &ret_ts_index);

   private:
    common::SimpleList<ChunkGroupMeta *> &chunk_group_meta_list_;
    common::SimpleList<ChunkGroupMeta *>::Iterator chunk_group_meta_iter_;
    common::SimpleList<ChunkMeta *>::Iterator chunk_meta_iter_;

    // timeseries measurenemnt chunk meta info
    // map <device_name, <measurement_name, vector<chunk_meta>>>
    std::map<std::shared_ptr<IDeviceID>,
             std::map<common::String, std::vector<ChunkMeta *>>>
        tsm_chunk_meta_info_;

    // device iterator
    std::map<std::shared_ptr<IDeviceID>,
             std::map<common::String, std::vector<ChunkMeta *>>>::iterator
        tsm_device_iter_;

    // measurement iterator
    std::map<common::String, std::vector<ChunkMeta *>>::iterator
        tsm_measurement_iter_;
};

/* =============== TsFile Index ================ */
struct IComparable {
    virtual ~IComparable() = default;
    virtual bool operator<(const IComparable &other) const = 0;
    virtual bool operator>(const IComparable &other) const = 0;
    virtual bool operator==(const IComparable &other) const = 0;
    virtual int compare(const IComparable &other) {
        if (this->operator<(other)) {
            return -1;
        } else if (this->operator==(other)) {
            return 0;
        } else {
            return 1;
        }
    }
    virtual std::string to_string() const = 0;
};

struct DeviceIDComparable : IComparable {
    std::shared_ptr<IDeviceID> device_id_;

    explicit DeviceIDComparable(const std::shared_ptr<IDeviceID> &device_id)
        : device_id_(device_id) {}

    bool operator<(const IComparable &other) const override {
        const auto *other_device =
            dynamic_cast<const DeviceIDComparable *>(&other);
        if (!other_device) throw std::runtime_error("Incompatible comparison");
        return *device_id_ < *other_device->device_id_;
    }

    bool operator>(const IComparable &other) const override {
        const auto *other_device =
            dynamic_cast<const DeviceIDComparable *>(&other);
        if (!other_device) throw std::runtime_error("Incompatible comparison");
        return *device_id_ != *other_device->device_id_ &&
               !(*device_id_ < *other_device->device_id_);
    }

    bool operator==(const IComparable &other) const override {
        const auto *other_device =
            dynamic_cast<const DeviceIDComparable *>(&other);
        if (!other_device) throw std::runtime_error("Incompatible comparison");
        return *device_id_ == *other_device->device_id_;
    }

    std::string to_string() const override {
        return device_id_->get_device_name();
    }
};

struct StringComparable : IComparable {
    std::string value_;

    explicit StringComparable(const std::string &value) : value_(value) {}

    bool operator<(const IComparable &other) const override {
        const auto *other_string =
            dynamic_cast<const StringComparable *>(&other);
        if (!other_string) throw std::runtime_error("Incompatible comparison");
        return value_ < other_string->value_;
    }

    bool operator>(const IComparable &other) const override {
        const auto *other_string =
            dynamic_cast<const StringComparable *>(&other);
        if (!other_string) throw std::runtime_error("Incompatible comparison");
        return value_ > other_string->value_;
    }

    bool operator==(const IComparable &other) const override {
        const auto *other_string =
            dynamic_cast<const StringComparable *>(&other);
        if (!other_string) throw std::runtime_error("Incompatible comparison");
        return value_ == other_string->value_;
    }

    std::string to_string() const override { return value_; }
};

struct IMetaIndexEntry {
    static void self_destructor(IMetaIndexEntry *ptr) {
        if (ptr) {
            ptr->~IMetaIndexEntry();
        }
    }
    IMetaIndexEntry() = default;
    virtual ~IMetaIndexEntry() = default;

    virtual int serialize_to(common::ByteStream &out) { return common::E_OK; }
    virtual int deserialize_from(common::ByteStream &out,
                                 common::PageArena *pa) {
        return common::E_NOT_SUPPORT;
    }
    virtual int64_t get_offset() const = 0;
    virtual bool is_device_level() const { return false; }
    virtual std::shared_ptr<IComparable> get_compare_key() const {
        return std::shared_ptr<IComparable>();
    }
    virtual common::String get_name() const { return {}; }
    virtual std::shared_ptr<IDeviceID> get_device_id() const { return nullptr; }
    virtual std::shared_ptr<IMetaIndexEntry> clone(common::PageArena *pa) = 0;
#ifndef NDEBUG
    virtual void print(std::ostream &os) const {}
    friend std::ostream &operator<<(std::ostream &os,
                                    const IMetaIndexEntry &entry) {
        entry.print(os);
        return os;
    }
#endif
};

struct DeviceMetaIndexEntry : IMetaIndexEntry {
    std::shared_ptr<IDeviceID> device_id_;
    int64_t offset_;

    DeviceMetaIndexEntry() = default;

    DeviceMetaIndexEntry(const std::shared_ptr<IDeviceID> &device_id,
                         const int64_t offset)
        : device_id_(device_id), offset_(offset) {}

    ~DeviceMetaIndexEntry() override = default;

    static void self_deleter(DeviceMetaIndexEntry *ptr) {
        if (ptr) {
            ptr->~DeviceMetaIndexEntry();
        }
    }

    int serialize_to(common::ByteStream &out) override {
        int ret = common::E_OK;
        if (RET_FAIL(device_id_->serialize(out))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::write_i64(offset_, out))) {
        }
        return ret;
    }

    std::shared_ptr<IDeviceID> &get_device_id() { return device_id_; }

    int deserialize_from(common::ByteStream &in,
                         common::PageArena *pa) override {
        int ret = common::E_OK;
        device_id_ = std::make_shared<StringArrayDeviceID>("init");
        if (RET_FAIL(device_id_->deserialize(in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_i64(offset_, in))) {
        }
        return ret;
    }

    int64_t get_offset() const override { return offset_; }

    std::shared_ptr<IComparable> get_compare_key() const override {
        return std::make_shared<DeviceIDComparable>(device_id_);
    }

    bool is_device_level() const override { return true; }
    common::String get_name() const override { return {}; }
    std::shared_ptr<IDeviceID> get_device_id() const override {
        return device_id_;
    }
    std::shared_ptr<IMetaIndexEntry> clone(common::PageArena *pa) override {
        return std::make_shared<DeviceMetaIndexEntry>(device_id_, offset_);
    }
#ifndef NDEBUG
    void print(std::ostream &os) const override {
        os << "name=" << device_id_ << ", offset=" << offset_;
    }
#endif
};

struct MeasurementMetaIndexEntry : IMetaIndexEntry {
    common::String name_;
    int64_t offset_;

    ~MeasurementMetaIndexEntry() override = default;

    MeasurementMetaIndexEntry() = default;
    MeasurementMetaIndexEntry(const common::String &name, const int64_t offset,
                              common::PageArena &pa) {
        offset_ = offset;
        name_.dup_from(name, pa);
    }

    FORCE_INLINE int init(const std::string &str, const int64_t offset,
                          common::PageArena &pa) {
        offset_ = offset;
        return name_.dup_from(str, pa);
    }

    int serialize_to(common::ByteStream &out) override {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_mystring(name_, out))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::write_i64(offset_, out))) {
        }
        return ret;
    }

    int deserialize_from(common::ByteStream &in,
                         common::PageArena *pa) override {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_mystring(name_, pa, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_i64(offset_, in))) {
        }
        return ret;
    }

    int64_t get_offset() const override { return offset_; }

    std::shared_ptr<IComparable> get_compare_key() const override {
        return std::make_shared<StringComparable>(name_.to_std_string());
    }

    bool is_device_level() const override { return false; }

    common::String get_name() const override { return name_; }
    std::shared_ptr<IDeviceID> get_device_id() const override {
        return nullptr;
    }
    std::shared_ptr<IMetaIndexEntry> clone(common::PageArena *pa) override {
        return std::make_shared<MeasurementMetaIndexEntry>(name_, offset_, *pa);
    }
#ifndef NDEBUG
    void print(std::ostream &os) const override {
        os << "name=" << name_ << ", offset=" << offset_;
    }
#endif
};

enum MetaIndexNodeType {
    INTERNAL_DEVICE = 0,
    LEAF_DEVICE = 1,
    INTERNAL_MEASUREMENT = 2,
    LEAF_MEASUREMENT = 3,
    INVALID_META_NODE_TYPE = 4,
};
#ifndef NDEBUG
static const char *meta_index_node_type_names[5] = {
    "INTERNAL_DEVICE", "LEAF_DEVICE", "INTERNAL_MEASUREMENT",
    "LEAF_MEASUREMENT", "INVALID_META_NODE_TYPE"};
#endif

struct MetaIndexNode {
    // TODO use vector to support binary search
    // common::SimpleList<MetaIndexEntry*> children_;
    std::vector<std::shared_ptr<IMetaIndexEntry>> children_;
    int64_t end_offset_;
    MetaIndexNodeType node_type_;
    common::PageArena *pa_;

    explicit MetaIndexNode(common::PageArena *pa)
        : children_(), end_offset_(0), node_type_(), pa_(pa) {}

    std::shared_ptr<IMetaIndexEntry> peek() {
        if (children_.empty()) {
            return nullptr;
        }
        return children_[0];
    }

    ~MetaIndexNode() {}

    static void self_deleter(MetaIndexNode *ptr) {
        if (ptr) {
            ptr->~MetaIndexNode();
        }
    }

    int binary_search_children(
        std::shared_ptr<IComparable> key, bool exact_search,
        std::shared_ptr<IMetaIndexEntry> &ret_index_entry,
        int64_t &ret_end_offset);

    int serialize_to(common::ByteStream &out) {
        int ret = common::E_OK;
#if DEBUG_SE
        int64_t start_pos = out.total_size();
#endif
        if (RET_FAIL(common::SerializationUtil::write_var_uint(children_.size(),
                                                               out))) {
        } else {
            for (size_t i = 0; IS_SUCC(ret) && i < children_.size(); i++) {
                auto entry = children_[i];
                if (RET_FAIL(entry->serialize_to(out))) {
                }
            }
            if (IS_SUCC(ret)) {
                if (RET_FAIL(common::SerializationUtil::write_i64(end_offset_,
                                                                  out))) {
                } else if (RET_FAIL(common::SerializationUtil::write_char(
                               node_type_, out))) {
                }
            }
        }
#if DEBUG_SE
        std::cout << "MetaIndexNode serialize_to. this=" << *this
                  << " at file pos: " << start_pos << " to " << out.total_size()
                  << std::endl;
#endif
        return ret;
    }

    int deserialize_from(const char *buf, int len) {
        common::ByteStream bs;
        bs.wrap_from(buf, len);
        return deserialize_from(bs);
    }
    int deserialize_from(common::ByteStream &in) {
        int ret = common::E_OK;
        uint32_t children_size = 0;
        if (RET_FAIL(
                common::SerializationUtil::read_var_uint(children_size, in))) {
            return ret;
        }
        for (uint32_t i = 0; i < children_size && IS_SUCC(ret); i++) {
            void *entry_buf = pa_->alloc(sizeof(MeasurementMetaIndexEntry));
            if (IS_NULL(entry_buf)) {
                return common::E_OOM;
            }
            auto entry = new (entry_buf) MeasurementMetaIndexEntry;

            if (RET_FAIL(entry->deserialize_from(in, pa_))) {
            } else {
                children_.push_back(std::shared_ptr<IMetaIndexEntry>(
                    entry, IMetaIndexEntry::self_destructor));
            }
        }  // end for
        if (IS_SUCC(ret)) {
            char node_type_ch = 0;
            if (RET_FAIL(
                    common::SerializationUtil::read_i64(end_offset_, in))) {
            } else if (RET_FAIL(common::SerializationUtil::read_char(
                           node_type_ch, in))) {
            } else {
                node_type_ = (MetaIndexNodeType)node_type_ch;
            }
        }
#if DEBUG_SE
        std::cout << "MetaIndexNode deserialize_from. this=" << *this
                  << std::endl;
#endif
        return ret;
    }
    int device_deserialize_from(const char *buf, int len) {
        common::ByteStream bs;
        bs.wrap_from(buf, len);
        return device_deserialize_from(bs);
    }
    int device_deserialize_from(common::ByteStream &in) {
        int ret = common::E_OK;
        uint32_t children_size = 0;
        if (RET_FAIL(
                common::SerializationUtil::read_var_uint(children_size, in))) {
            return ret;
        }
        for (uint32_t i = 0; i < children_size && IS_SUCC(ret); i++) {
            void *entry_buf = pa_->alloc(sizeof(DeviceMetaIndexEntry));
            if (IS_NULL(entry_buf)) {
                return common::E_OOM;
            }
            auto *entry_ptr = new (entry_buf) DeviceMetaIndexEntry();
            auto entry = std::shared_ptr<DeviceMetaIndexEntry>(
                entry_ptr, DeviceMetaIndexEntry::self_deleter);
            if (RET_FAIL(entry->deserialize_from(in, pa_))) {
            } else {
                children_.push_back(entry);
            }
        }  // end for
        if (IS_SUCC(ret)) {
            char node_type_ch = 0;
            if (RET_FAIL(
                    common::SerializationUtil::read_i64(end_offset_, in))) {
            } else if (RET_FAIL(common::SerializationUtil::read_char(
                           node_type_ch, in))) {
            } else {
                node_type_ = (MetaIndexNodeType)node_type_ch;
            }
        }
#if DEBUG_SE
        std::cout << "MetaIndexNode deserialize_from. this=" << *this
                  << std::endl;
#endif
        return ret;
    }

#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os,
                                    const MetaIndexNode &node) {
        os << "end_offset=" << node.end_offset_
           << ", node_type=" << meta_index_node_type_names[node.node_type_];

        os << ", MetaIndexEntry children={";
        for (size_t i = 0; i < node.children_.size(); i++) {
            os << (i == 0 ? "" : ", ") << "[" << i << "]={"
               << *node.children_[i] << "}";
        }
        os << "}";
        return os;
    }
#endif

    FORCE_INLINE bool is_full() const {
        return children_.size() >=
               common::g_config_value_.max_degree_of_index_node_;
    }

    FORCE_INLINE bool is_empty() const { return children_.size() == 0; }

    FORCE_INLINE int push_entry(std::shared_ptr<IMetaIndexEntry> entry) {
#if DEBUG_SE
        std::cout << "MetaIndexNode.push_entry(" << *entry << ")" << std::endl;
#endif
        children_.push_back(entry);
        return common::E_OK;
    }
    FORCE_INLINE void destroy() {
        // std::vector<MetaIndexEntry*>().swap(children_);
        children_.~vector();
    }
};

class TableSchema;

struct TsFileMeta {
    typedef std::map<std::shared_ptr<IDeviceID>, std::shared_ptr<MetaIndexNode>,
                     IDeviceIDComparator>
        DeviceNodeMap;
    std::map<std::string, std::shared_ptr<MetaIndexNode>>
        table_metadata_index_node_map_;
    std::unordered_map<std::string, std::string *> tsfile_properties_;
    typedef std::unordered_map<std::string, std::shared_ptr<TableSchema>>
        TableSchemasMap;
    TableSchemasMap table_schemas_;
    int64_t meta_offset_;
    BloomFilter *bloom_filter_;
    common::PageArena *page_arena_;

    int get_table_metaindex_node(const std::string &table_name,
                                 MetaIndexNode *&ret_node) {
        std::map<std::string, std::shared_ptr<MetaIndexNode>>::iterator it =
            table_metadata_index_node_map_.find(table_name);
        if (it == table_metadata_index_node_map_.end()) {
            return common::E_TABLE_NOT_EXIST;
        }
        ret_node = it->second.get();
        return common::E_OK;
    }

    int get_table_schema(const std::string &table_name,
                         std::shared_ptr<TableSchema> &ret_schema) {
        TableSchemasMap::iterator it = table_schemas_.find(table_name);
        if (it == table_schemas_.end()) {
            return common::E_TABLE_NOT_EXIST;
        }
        ret_schema = it->second;
        return common::E_OK;
    }

    TsFileMeta()
        : meta_offset_(0), bloom_filter_(nullptr), page_arena_(nullptr) {}

    explicit TsFileMeta(common::PageArena *pa)
        : meta_offset_(0), bloom_filter_(nullptr), page_arena_(pa) {}
    ~TsFileMeta() {
        if (bloom_filter_ != nullptr) {
            bloom_filter_->destroy();
        }
        for (auto properties : tsfile_properties_) {
            if (properties.second != nullptr) {
                delete properties.second;
            }
        }
        table_metadata_index_node_map_.clear();
        table_schemas_.clear();
    }

    int serialize_to(common::ByteStream &out);

    int deserialize_from(common::ByteStream &in);

#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os,
                                    const TsFileMeta &tsfile_meta) {
        os << "meta_offset=" << tsfile_meta.meta_offset_;
        return os;
    }
#endif
};

// Timeseries ID and its [start_time, end_time] in a tsfile
struct TimeseriesTimeIndexEntry {
    common::TsID ts_id_;
    TimeRange time_range_;
};

}  // end namespace storage
#endif  // COMMON_TSFILE_COMMON_H
