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

#ifndef READER_FILTER_TAG_FILTER_H
#define READER_FILTER_TAG_FILTER_H

#include <memory>
#include <string>
#include <vector>

#include "common/allocator/my_string.h"
#include "common/schema.h"
#include "reader/filter/filter.h"

struct table_schema;
namespace storage {
class TagFilter : public Filter {
   public:
    TagFilter(int col_idx, std::string tag_value);
    ~TagFilter() override;

    virtual bool satisfyRow(int time, std::vector<std::string*> segments) const;
    virtual bool satisfyRow(std::vector<std::string*> segments) const;

    std::string value_;
    std::string value2_;  // For range queries
    int col_idx_;
};

// Equality comparison
class TagEq : public TagFilter {
   public:
    TagEq(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Inequality comparison
class TagNeq : public TagFilter {
   public:
    TagNeq(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Less than comparison
class TagLt : public TagFilter {
   public:
    TagLt(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Less than or equal comparison
class TagLteq : public TagFilter {
   public:
    TagLteq(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Greater than comparison
class TagGt : public TagFilter {
   public:
    TagGt(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Greater than or equal comparison
class TagGteq : public TagFilter {
   public:
    TagGteq(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Regular expression match
class TagRegExp : public TagFilter {
    std::regex pattern_;
    bool is_valid_pattern_ = false;

   public:
    TagRegExp(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Regular expression not match
class TagNotRegExp : public TagFilter {
    std::regex pattern_;
    bool is_valid_pattern_ = false;

   public:
    TagNotRegExp(int col_idx, std::string tag_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Range query [value_, value2_]
class TagBetween : public TagFilter {
   public:
    TagBetween(int col_idx, std::string lower_value, std::string upper_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Not in range
class TagNotBetween : public TagFilter {
   public:
    TagNotBetween(int col_idx, std::string lower_value,
                  std::string upper_value);
    bool satisfyRow(std::vector<std::string*> segments) const override;
};

// Logical AND operation (binary)
class TagAnd : public TagFilter {
   public:
    TagAnd(TagFilter* left, TagFilter* right);
    ~TagAnd() override;
    bool satisfyRow(std::vector<std::string*> segments) const override;

   private:
    TagFilter* left_;
    TagFilter* right_;
};

// Logical OR operation (binary)
class TagOr : public TagFilter {
   public:
    TagOr(TagFilter* left, TagFilter* right);
    ~TagOr() override;
    bool satisfyRow(std::vector<std::string*> segments) const override;

   private:
    TagFilter* left_;
    TagFilter* right_;
};

// Logical NOT operation
class TagNot : public TagFilter {
   public:
    explicit TagNot(TagFilter* filter);
    ~TagNot() override;
    bool satisfyRow(std::vector<std::string*> segments) const override;

   private:
    TagFilter* filter_;
};

class TagFilterBuilder {
    TableSchema* table_schema_;

   public:
    explicit TagFilterBuilder(TableSchema* schema);

    Filter* eq(const std::string& columnName, const std::string& value);
    Filter* neq(const std::string& columnName, const std::string& value);
    Filter* lt(const std::string& columnName, const std::string& value);
    Filter* lteq(const std::string& columnName, const std::string& value);
    Filter* gt(const std::string& columnName, const std::string& value);
    Filter* gteq(const std::string& columnName, const std::string& value);
    Filter* reg_exp(const std::string& columnName, const std::string& value);
    Filter* not_reg_exp(const std::string& columnName,
                        const std::string& value);
    Filter* between_and(const std::string& columnName, const std::string& lower,
                        const std::string& upper);
    Filter* not_between_and(const std::string& columnName,
                            const std::string& lower, const std::string& upper);

    // Logical operations
    static Filter* and_filter(Filter* left, Filter* right);
    static Filter* or_filter(Filter* left, Filter* right);
    static Filter* not_filter(Filter* filter);

   private:
    int get_id_column_index(const std::string& columnName);
};

}  // namespace storage
#endif  // READER_FILTER_TAG_FILTER_H