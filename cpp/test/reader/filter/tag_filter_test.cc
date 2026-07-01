/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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

#include "reader/filter/tag_filter.h"

#include <gtest/gtest.h>

#include "common/schema.h"

using namespace storage;

class TagFilterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::vector<common::ColumnSchema> column_schemas;
        column_schemas.emplace_back("name", common::TSDataType::TEXT,
                                    common::ColumnCategory::TAG);
        column_schemas.emplace_back("age", common::TSDataType::TEXT,
                                    common::ColumnCategory::TAG);
        column_schemas.emplace_back("department", common::TSDataType::TEXT,
                                    common::ColumnCategory::TAG);
        column_schemas.emplace_back("status", common::TSDataType::TEXT,
                                    common::ColumnCategory::TAG);
        column_schemas.emplace_back("score", common::TSDataType::TEXT,
                                    common::ColumnCategory::TAG);

        schema_ = new TableSchema("test_table", column_schemas);
        builder_ = new TagFilterBuilder(schema_);
    }

    void TearDown() override {
        delete builder_;
        delete schema_;
    }

    // Helper method to create segments starting from index 1
    static std::vector<std::string*> createSegments(
        const std::string& name, const std::string& age,
        const std::string& department, const std::string& status = "",
        const std::string& score = "") {
        std::vector<std::string*> segments;
        segments.emplace_back(nullptr);  // index 0 - placeholder or device name
        segments.push_back(new std::string(name));
        segments.push_back(new std::string(age));
        segments.push_back(new std::string(department));
        segments.push_back(new std::string(status));
        segments.push_back(new std::string(score));
        return segments;
    }

    // Helper method to cleanup segments
    static void cleanupSegments(std::vector<std::string*>& segments) {
        for (size_t i = 1; i < segments.size(); i++) {
            delete segments[i];
        }
    }

    TableSchema* schema_ = nullptr;
    TagFilterBuilder* builder_ = nullptr;
};

// Equality filter
TEST_F(TagFilterTest, TagEqFilter) {
    auto filter = builder_->eq("name", "john");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("alice", "25", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// Inequality filter
TEST_F(TagFilterTest, TagNeqFilter) {
    auto filter = builder_->neq("name", "john");
    ASSERT_NE(filter, nullptr);

    auto segments =
        createSegments("alice", "25", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// Less than filter
TEST_F(TagFilterTest, TagLtFilter) {
    auto filter = builder_->lt("age", "30");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "35", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// Less than or equal filter
TEST_F(TagFilterTest, TagLteqFilter) {
    auto filter = builder_->lteq("age", "30");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "30", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "35", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// Greater than filter
TEST_F(TagFilterTest, TagGtFilter) {
    auto filter = builder_->gt("age", "30");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "35", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// Greater than or equal filter
TEST_F(TagFilterTest, TagGteqFilter) {
    auto filter = builder_->gteq("age", "30");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "30", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "35", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// Between filter
TEST_F(TagFilterTest, TagBetweenFilter) {
    auto filter = builder_->between_and("age", "25", "35");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "30", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "35", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "20", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "40", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// Not between filter
TEST_F(TagFilterTest, TagNotBetweenFilter) {
    auto filter = builder_->not_between_and("age", "25", "35");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "20", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "40", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "30", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
}

// AND filter
TEST_F(TagFilterTest, TagAndFilter) {
    auto left_filter = builder_->gteq("age", "25");
    auto right_filter = builder_->eq("department", "engineering");
    auto and_filter = builder_->and_filter(left_filter, right_filter);
    ASSERT_NE(and_filter, nullptr);

    auto segments = createSegments("john", "30", "engineering", "active", "95");
    EXPECT_TRUE(and_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "20", "engineering", "active", "95");
    EXPECT_FALSE(and_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "30", "sales", "active", "95");
    EXPECT_FALSE(and_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete and_filter;
}

// OR filter
TEST_F(TagFilterTest, TagOrFilter) {
    auto left_filter = builder_->lt("age", "25");
    auto right_filter = builder_->eq("department", "engineering");
    auto or_filter = builder_->or_filter(left_filter, right_filter);
    ASSERT_NE(or_filter, nullptr);

    auto segments = createSegments("john", "20", "engineering", "active", "95");
    EXPECT_TRUE(or_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "20", "sales", "active", "95");
    EXPECT_TRUE(or_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "30", "engineering", "active", "95");
    EXPECT_TRUE(or_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "30", "sales", "active", "95");
    EXPECT_FALSE(or_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete or_filter;
}

// NOT filter
TEST_F(TagFilterTest, TagNotFilter) {
    auto base_filter = builder_->eq("status", "active");
    auto not_filter = builder_->not_filter(base_filter);
    ASSERT_NE(not_filter, nullptr);

    auto segments =
        createSegments("john", "30", "engineering", "inactive", "95");
    EXPECT_TRUE(not_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("john", "30", "engineering", "active", "95");
    EXPECT_FALSE(not_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete not_filter;
}

// Complex nested filters
TEST_F(TagFilterTest, ComplexNestedFilters) {
    auto age_filter = builder_->gteq("age", "25");
    auto dept_filter = builder_->eq("department", "engineering");
    auto score_filter = builder_->gt("score", "90");

    auto and_filter = builder_->and_filter(age_filter, dept_filter);
    auto complex_filter = builder_->or_filter(and_filter, score_filter);
    ASSERT_NE(complex_filter, nullptr);

    auto segments = createSegments("john", "30", "engineering", "active", "85");
    EXPECT_TRUE(complex_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("alice", "20", "sales", "active", "95");
    EXPECT_TRUE(complex_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("bob", "20", "sales", "active", "85");
    EXPECT_FALSE(complex_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete complex_filter;
}

// Invalid column name
TEST_F(TagFilterTest, InvalidColumnName) {
    auto filter = builder_->eq("invalid_column", "value");
    EXPECT_EQ(filter, nullptr);
}

// Boundary conditions
TEST_F(TagFilterTest, BoundaryConditions) {
    auto filter = builder_->eq("name", "test");
    ASSERT_NE(filter, nullptr);

    std::vector<std::string*> empty_segments;
    EXPECT_FALSE(filter->satisfyRow(0, empty_segments));

    std::vector<std::string*> small_segments = {nullptr};
    EXPECT_FALSE(filter->satisfyRow(0, small_segments));

    std::vector<std::string*> minimal_segments = {nullptr,
                                                  new std::string("test")};
    EXPECT_TRUE(filter->satisfyRow(0, minimal_segments));
    delete minimal_segments[1];

    delete filter;
}

// Basic regex match and not match
TEST_F(TagFilterTest, TagRegExpBasic) {
    auto filter = builder_->reg_exp("name", "^j.*");
    ASSERT_NE(filter, nullptr);

    auto segments = createSegments("john", "25", "engineering", "active", "95");
    EXPECT_TRUE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    segments = createSegments("alice", "25", "engineering", "active", "95");
    EXPECT_FALSE(filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    auto not_filter = builder_->not_reg_exp("name", "^j.*");
    segments = createSegments("alice", "25", "engineering", "active", "95");
    EXPECT_TRUE(not_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete filter;
    delete not_filter;
}

// Complex regex pattern with logical operations
TEST_F(TagFilterTest, TagRegExpComplexLogic) {
    // Match names starting with j OR age in 20s AND department contains "eng"
    auto name_filter = builder_->reg_exp("name", "^j.*");
    auto age_filter = builder_->reg_exp("age", "^2[0-9]$");
    auto dept_filter = builder_->reg_exp("department", ".*eng.*");

    auto age_dept_and = builder_->and_filter(age_filter, dept_filter);
    auto complex_or = builder_->or_filter(name_filter, age_dept_and);

    auto segments = createSegments("john", "35", "sales", "active", "95");
    EXPECT_TRUE(complex_or->satisfyRow(0, segments));  // name matches
    cleanupSegments(segments);

    segments = createSegments("alice", "25", "engineering", "active", "95");
    EXPECT_TRUE(complex_or->satisfyRow(0, segments));  // age and dept match
    cleanupSegments(segments);

    segments = createSegments("bob", "35", "sales", "active", "95");
    EXPECT_FALSE(complex_or->satisfyRow(0, segments));  // no match
    cleanupSegments(segments);

    delete complex_or;
}

// Edge cases: invalid regex and boundary values
TEST_F(TagFilterTest, TagRegExpEdgeCases) {
    // Invalid regex should not crash
    auto invalid_filter = builder_->reg_exp("name", "[invalid[pattern");
    ASSERT_NE(invalid_filter, nullptr);

    auto segments = createSegments("test", "25", "engineering", "active", "95");
    EXPECT_FALSE(
        invalid_filter->satisfyRow(0, segments));  // handles gracefully
    cleanupSegments(segments);

    // Empty pattern matches everything
    auto empty_filter = builder_->reg_exp("name", "");
    segments = createSegments("any", "25", "engineering", "active", "95");
    EXPECT_TRUE(empty_filter->satisfyRow(0, segments));
    cleanupSegments(segments);

    delete invalid_filter;
    delete empty_filter;
}