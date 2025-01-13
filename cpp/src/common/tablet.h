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

#ifndef COMMON_TABLET_H
#define COMMON_TABLET_H

#include <algorithm>
#include <memory>
#include <vector>

#include "common/container/bit_map.h"
#include "schema.h"

namespace storage {

template <typename T>

class TabletRowIterator;
class TabletColIterator;

class Tablet {
   public:
    static const uint32_t DEFAULT_MAX_ROWS = 1024;

   public:
    Tablet(const std::string &device_id,
           std::shared_ptr<std::vector<MeasurementSchema>> schema_vec,
           int max_rows = DEFAULT_MAX_ROWS)
        : max_row_num_(max_rows),
          device_id_(device_id),
          schema_vec_(schema_vec),
          timestamps_(NULL),
          value_matrix_(NULL),
          bitmaps_(NULL) {
        ASSERT(device_id.size() >= 1);
        ASSERT(schema_vec != NULL);
        ASSERT(max_rows > 0 && max_rows < (1 << 30));
        if (max_rows < 0) {
            ASSERT(false);
            max_row_num_ = DEFAULT_MAX_ROWS;
        }
    }

    Tablet(const std::string &device_id,
           const std::vector<std::string> *measurement_list,
           const std::vector<common::TSDataType> *data_type_list,
           int max_row_num = DEFAULT_MAX_ROWS)
        : max_row_num_(max_row_num),
          device_id_(device_id),
          timestamps_(NULL),
          value_matrix_(NULL),
          bitmaps_(NULL) {
        ASSERT(device_id.size() >= 1);
        ASSERT(measurement_list != NULL);
        ASSERT(data_type_list != NULL);
        ASSERT(max_row_num > 0 && max_row_num < (1 << 30));
        if (max_row_num < 0) {
            ASSERT(false);
            max_row_num_ = DEFAULT_MAX_ROWS;
        }

        ASSERT(measurement_list->size() == data_type_list->size());
        std::vector<MeasurementSchema> measurement_vec;
        measurement_vec.reserve(measurement_list->size());
        std::transform(measurement_list->begin(), measurement_list->end(),
                       data_type_list->begin(),
                       std::back_inserter(measurement_vec),
                       [](const std::string &name, common::TSDataType type) {
                           return MeasurementSchema(name, type);
                       });
    }
    ~Tablet() { destroy(); }

    int init();
    void destroy();
    size_t get_column_count() const { return schema_vec_->size(); }

    int add_timestamp(uint32_t row_index, int64_t timestamp);

    template <typename T>
    int add_value(uint32_t row_index, uint32_t schema_index, T val);

    template <typename T>
    int add_value(uint32_t row_index, const std::string &measurement_name,
                  T val);

    friend class TabletColIterator;
    friend class TsFileWriter;
    friend struct MeasurementNamesFromTablet;

   private:
    typedef std::map<std::string, int>::iterator SchemaMapIterator;

   private:
    template <typename T>
    void process_val(uint32_t row_index, uint32_t schema_index, T val);
    int max_row_num_;
    std::string device_id_;
    std::shared_ptr<std::vector<MeasurementSchema>> schema_vec_;
    std::map<std::string, int> schema_map_;
    int64_t *timestamps_;
    void **value_matrix_;
    common::BitMap *bitmaps_;
};

}  // end namespace storage
#endif  // COMMON_TABLET_H
