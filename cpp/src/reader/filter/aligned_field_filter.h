/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#ifndef READER_FILTER_ALIGNED_FIELD_FILTER_H
#define READER_FILTER_ALIGNED_FIELD_FILTER_H

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/tsblock/tsblock.h"
#include "reader/filter/filter.h"
#include "reader/filter/selection_mask.h"

namespace storage {
class TableSchema;

// Column-aware FIELD predicate. Aligned sources use batch evaluation and page
// pruning when possible; non-aligned sources use the table reader's timestamp
// merge and typed row evaluation. Mixed-device files may use both paths.
// It deliberately does not implement TableFieldFilter::satisfy_row().
class AlignedFieldFilter : public Filter {
   public:
    enum class Kind { COMPARISON, IS_NULL, IS_NOT_NULL, AND, OR, NOT };

    enum class CompareOp { EQ, NE, LT, LE, GT, GE };

    struct ColumnView {
        std::string name;
        common::TSDataType type = common::INVALID_DATATYPE;
        const char* fixed_values = nullptr;
        const common::String* string_values = nullptr;
        const uint8_t* notnull_bitmap = nullptr;
        uint32_t notnull_bitmap_size = 0;
        uint32_t row_offset = 0;
        uint32_t nonnull_start = 0;
    };

    struct Literal {
        common::TSDataType type = common::INVALID_DATATYPE;
        bool bool_value = false;
        int32_t int32_value = 0;
        int64_t int64_value = 0;
        float float_value = 0.0F;
        double double_value = 0.0;
        std::string string_value;

        static Literal boolean(bool value);
        static Literal int32(int32_t value);
        static Literal int64(int64_t value);
        static Literal float32(float value);
        static Literal float64(double value);
        static Literal string(std::string value);
    };

    using Ptr = std::unique_ptr<AlignedFieldFilter>;

    ~AlignedFieldFilter() override = default;

    static Ptr compare(std::string column, CompareOp op, Literal literal);
    static Ptr is_null(std::string column);
    static Ptr is_not_null(std::string column);
    // Logical operands must be non-null. A missing operand remains an invalid
    // non-null tree, rejected with E_INVALID_ARG when bound or queried.
    static Ptr and_filter(Ptr left, Ptr right);
    static Ptr or_filter(Ptr left, Ptr right);
    static Ptr not_filter(Ptr child);

    Kind kind() const { return kind_; }
    CompareOp compare_op() const { return compare_op_; }
    const std::string& column() const { return column_; }
    const Literal& literal() const { return literal_; }

    std::vector<std::string> referenced_columns() const;

    // False when a composite node is missing one of its operands. Callers use
    // this to reject a malformed tree before page statistics can classify it
    // as a full-page match and skip evaluation.
    bool is_valid() const;

    // Bind once before execution: validate FIELD references and literal types,
    // and convert numeric constants to the declared column type using Java's
    // Number.intValue/longValue/floatValue/doubleValue rules. Comparisons and
    // statistics pruning then use the same normalized constant, even on empty
    // or all-NULL inputs.
    int bind(TableSchema& schema);

    // Conservatively decides whether any row in a page can satisfy the filter.
    // Missing or unsupported statistics always return true.
    bool may_match(const std::map<std::string, Statistic*>& statistics,
                   int64_t row_count) const;
    bool all_match(const std::map<std::string, Statistic*>& statistics,
                   int64_t row_count) const;

    // Evaluates against block.get_vector(0) as time and column_names[i] as
    // block.get_vector(i + 1). The mask is reset to block row count.
    int evaluate(common::TsBlock& block,
                 const std::vector<std::string>& column_names,
                 SelectionMask& mask) const;
    int evaluate_columns(const std::vector<ColumnView>& columns,
                         uint32_t row_count, SelectionMask& mask) const;

   private:
    AlignedFieldFilter(Kind kind, std::string column, CompareOp op,
                       Literal literal, bool negated = false);
    AlignedFieldFilter(Kind kind, Ptr left, Ptr right);
    // Push NOT down to the leaves so negation keeps SQL three-valued NULL
    // semantics; a single SelectionMask bit cannot represent NULL vs FALSE.
    static Ptr negate(Ptr node);

    int evaluate_leaf(common::TsBlock& block,
                      const std::map<std::string, uint32_t>& column_index,
                      SelectionMask& mask) const;
    int evaluate_leaf(const ColumnView& column, uint32_t row_count,
                      SelectionMask& mask) const;
    Kind kind_;
    std::string column_;
    CompareOp compare_op_ = CompareOp::EQ;
    Literal literal_;
    // True when this leaf negates an ordered comparison. Preserve it until
    // binding identifies the column type. Flipping the operator is not
    // equivalent for FLOAT/DOUBLE: NaN makes
    // every ordered comparison false, so the negation must be evaluated as
    // !(value <op> literal). NULL still never satisfies either form.
    bool negated_ = false;
    Ptr left_;
    Ptr right_;
};

}  // namespace storage

#endif  // READER_FILTER_ALIGNED_FIELD_FILTER_H
