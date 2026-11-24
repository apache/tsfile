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
#ifndef COMMON_ARROW_BATCH_H
#define COMMON_ARROW_BATCH_H

#include <memory>
#include <vector>

#include "common/arrow_c_data.h"
#include "common/tsblock/tsblock.h"

namespace common {

// Mutable only while a batch is being built. Nulls have physical value slots;
// bitmaps use Arrow's least-significant-bit-first layout.
class ArrowColumn {
   public:
    ArrowColumn(TSDataType type, uint32_t capacity);
    TSDataType type() const { return type_; }
    uint32_t capacity() const { return capacity_; }
    bool is_null(uint32_t row) const;
    void set_valid(uint32_t row);
    void set_valid_range(uint32_t begin, uint32_t count);
    char* mutable_values() { return reinterpret_cast<char*>(values_.data()); }
    const char* values() const {
        return reinterpret_cast<const char*>(values_.data());
    }
    uint8_t* mutable_validity() { return validity_.data(); }
    const uint8_t* validity() const { return validity_.data(); }
    const int32_t* offsets() const { return offsets_.data(); }
    const char* bytes() const { return bytes_.data(); }
    bool variable() const;
    // Return TsFile's native representation (DATE and BOOLEAN use scratch).
    const char* native_value(uint32_t row, char* scratch, uint32_t& size) const;
    int set_native(uint32_t row, TSDataType source_type, const char* data,
                   uint32_t size);
    int copy_selected(const ArrowColumn& source,
                      const std::vector<uint32_t>& rows, uint32_t begin,
                      uint32_t count, uint32_t destination);
    void finish(uint32_t rows);

   private:
    TSDataType type_;
    uint32_t capacity_;
    uint32_t string_rows_ = 0;
    std::vector<uint8_t> validity_;
    std::vector<uint64_t> values_;
    std::vector<int32_t> offsets_;
    std::vector<char> bytes_;
};

// Data ownership travels with the exported ArrowArray, independently of the
// reader. Exporting wraps buffers; it does not copy or convert values.
class ArrowBatch : public std::enable_shared_from_this<ArrowBatch> {
   public:
    ArrowBatch(const std::vector<std::string>& names,
               const std::vector<TSDataType>& types, uint32_t capacity);
    std::vector<std::string> names;
    std::vector<std::shared_ptr<ArrowColumn>> columns;
    uint32_t row_count = 0;
    void finish();
    int export_to(ArrowArray* array, ArrowSchema* schema);
    int to_tsblock(TupleDesc& desc, TsBlock*& block) const;
};

}  // namespace common
#endif
