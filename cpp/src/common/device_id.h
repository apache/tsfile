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

#ifndef COMMON_DEVICE_ID_H
#define COMMON_DEVICE_ID_H

#include <memory>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"

namespace storage {

class IDeviceID {
   public:
    virtual ~IDeviceID() = default;
    virtual int serialize(common::ByteStream& write_stream);
    virtual int deserialize(common::ByteStream& read_stream);
    virtual std::string get_table_name();
    virtual int segment_num();
    virtual const std::vector<std::string*>& get_segments() const;
    virtual std::string get_device_name() const;
    virtual bool operator<(const IDeviceID& other);
    virtual bool operator==(const IDeviceID& other);
    virtual bool operator!=(const IDeviceID& other);
    virtual std::string* get_split_segname_at(int pos) { return nullptr; }
    virtual int get_split_seg_num() { return 0; }
    virtual void split_table_name() {}

    /**
     * Splits a string by delimiter while respecting quoted sections.
     * Handles three quote types: backticks (`), double quotes ("), and single
     * quotes ('). Delimiters inside quoted sections are treated as part of the
     * token.
     *
     * Examples:
     * - "a.b.c" -> ["a", "b", "c"] (with delimiter '.')
     * - "a.`b.c`.d" -> ["a", "`b.c`", "d"] (preserves quotes, treats "b.c" as
     * one token)
     * - "table.`select`" -> ["table", "select"] (unquotes keywords in
     * backticks)
     *
     * @param str The input string to split
     * @param delimiter The character to split on (typically '.' for paths)
     * @return Vector of tokens, with quotes preserved except for keyword
     * unquoting
     * @throws std::runtime_error if newlines found, quotes unmatched, or
     * invalid identifier
     */
    static std::vector<std::string> split_string(const std::string& str,
                                                 char delimiter);
    /**
     * Removes surrounding quotes from identifiers under specific conditions.
     * Only unquotes backtick-enclosed identifiers if the inner content is a
     * reserved keyword. Preserves quotes for all other cases (non-keywords,
     * other quote types).
     *
     * Examples:
     * - "`select`" -> "select" (keyword in backticks gets unquoted)
     * - "`custom`" -> "`custom`" (non-keyword preserves backticks)
     * - "'select'" -> "'select'" (single quotes always preserved)
     * - "\"table\"" -> "\"table\"" (double quotes always preserved)
     *
     * @param identifier The potentially quoted identifier
     * @return Unquoted string if conditions met, otherwise original identifier
     */
    static std::string unquote_identifier(const std::string& identifier);
    /**
     * Validates identifier syntax according to specific rules.
     * Quoted identifiers are always accepted. Unquoted identifiers must:
     * - Not be pure digits (e.g., "123" is illegal)
     * - Not start with a digit (e.g., "1table" is illegal)
     * - Not contain wildcard '%' characters
     * - If containing '*', must consist only of asterisks (e.g., "*", "**",
     * "***" are allowed)
     *
     * Examples of illegal unquoted identifiers:
     * - "123" (pure digits)
     * - "1column" (starts with digit)
     * - "col%name" (contains wildcard)
     * - "abc*" (contains asterisk but also other characters)
     * - "a*b" (contains asterisk mixed with other characters)
     *
     * Examples of legal identifiers:
     * - "table1" (normal identifier)
     * - "*" (single asterisk wildcard)
     * - "**" (multiple asterisks)
     * - "***" (any number of consecutive asterisks)
     * - "`123`" (quoted digits are allowed)
     * - "`col%name`" (quoted wildcards allowed)
     * - "`abc*`" (quoted asterisk allowed in any position)
     *
     * @param identifier The identifier to validate
     * @throws std::runtime_error if identifier violates validation rules
     */
    static void validate_identifier(const std::string& identifier);

   protected:
    IDeviceID();

   private:
    const std::vector<std::string*> empty_segments_;
};

struct IDeviceIDComparator {
    bool operator()(const std::shared_ptr<IDeviceID>& lhs,
                    const std::shared_ptr<IDeviceID>& rhs) const;
};

class StringArrayDeviceID : public IDeviceID {
   public:
    explicit StringArrayDeviceID(const std::vector<std::string>& segments);
    explicit StringArrayDeviceID(const std::string& device_id_string);
    explicit StringArrayDeviceID(const std::vector<std::string*>& segments);
    explicit StringArrayDeviceID();
    ~StringArrayDeviceID() override;

    std::string get_device_name() const override;
    int serialize(common::ByteStream& write_stream) override;
    int deserialize(common::ByteStream& read_stream) override;
    std::string get_table_name() override;
    int segment_num() override;
    const std::vector<std::string*>& get_segments() const override;
    bool operator<(const IDeviceID& other) override;
    bool operator==(const IDeviceID& other) override;
    bool operator!=(const IDeviceID& other) override;

    void split_table_name() override { init_prefix_segments(); }

    std::string* get_split_segname_at(int pos) override {
        if (prefix_segments_.size() == 0 || prefix_segments_.size() == 1) {
            return segments_[pos];
        } else {
            if (pos < prefix_segments_.size()) {
                return prefix_segments_[pos];
            } else {
                return segments_[pos - prefix_segments_.size() + 1];
            }
        }
    }

    int get_split_seg_num() override {
        return prefix_segments_.size() == 0
                   ? segments_.size()
                   : segments_.size() + prefix_segments_.size() - 1;
    }

   private:
    std::vector<std::string*> segments_;

    std::vector<std::string*> prefix_segments_;

    void init_prefix_segments();

    static std::vector<std::string*> formalize(
        const std::vector<std::string>& segments);
    static std::vector<std::string> split_device_id_string(
        const std::string& device_id_string);
    static std::vector<std::string> split_device_id_string(
        const std::vector<std::string>& splits);
};

}  // namespace storage

#endif