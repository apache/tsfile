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
#include "common/tsfile_common.h"

#include <common/schema.h>
#include <cwrapper/TsFile-cwrapper.h>
#include <gtest/gtest.h>

namespace storage {
TEST(PageHeaderTest, DefaultConstructor) {
  PageHeader header;
  EXPECT_EQ(header.uncompressed_size_, 0);
  EXPECT_EQ(header.compressed_size_, 0);
  EXPECT_EQ(header.statistic_, nullptr);
}

TEST(PageHeaderTest, Reset) {
  PageHeader header;
  header.uncompressed_size_ = 100;
  header.compressed_size_ = 50;
  header.statistic_ = StatisticFactory::alloc_statistic(common::BOOLEAN);

  header.reset();
  EXPECT_EQ(header.uncompressed_size_, 0);
  EXPECT_EQ(header.compressed_size_, 0);
}

TEST(ChunkHeaderTest, DefaultConstructor) {
  ChunkHeader header;
  EXPECT_EQ(header.measurement_name_, "");
  EXPECT_EQ(header.data_size_, 0);
  EXPECT_EQ(header.data_type_, common::INVALID_DATATYPE);
  EXPECT_EQ(header.compression_type_, common::INVALID_COMPRESSION);
  EXPECT_EQ(header.encoding_type_, common::INVALID_ENCODING);
  EXPECT_EQ(header.num_of_pages_, 0);
  EXPECT_EQ(header.serialized_size_, 0);
  EXPECT_EQ(header.chunk_type_, 0);
}

TEST(ChunkHeaderTest, Reset) {
  ChunkHeader header;
  header.measurement_name_ = "test";
  header.data_size_ = 100;
  header.data_type_ = common::TSDataType::INT32;
  header.compression_type_ = common::CompressionType::SNAPPY;
  header.encoding_type_ = common::TSEncoding::PLAIN;
  header.num_of_pages_ = 5;
  header.serialized_size_ = 50;
  header.chunk_type_ = 1;

  header.reset();
  EXPECT_EQ(header.measurement_name_, "");
  EXPECT_EQ(header.data_size_, 0);
  EXPECT_EQ(header.data_type_, common::INVALID_DATATYPE);
  EXPECT_EQ(header.compression_type_, common::INVALID_COMPRESSION);
  EXPECT_EQ(header.encoding_type_, common::INVALID_ENCODING);
  EXPECT_EQ(header.num_of_pages_, 0);
  EXPECT_EQ(header.serialized_size_, 0);
  EXPECT_EQ(header.chunk_type_, 0);
}

TEST(ChunkMetaTest, DefaultConstructor) {
  ChunkMeta meta;
  EXPECT_EQ(meta.offset_of_chunk_header_, 0);
  EXPECT_EQ(meta.statistic_, nullptr);
  EXPECT_EQ(meta.mask_, 0);
}

TEST(ChunkMetaTest, Init) {
  ChunkMeta meta;
  char name[] = "test";
  common::String measurement_name(name, sizeof(name));
  Statistic stat;
  common::TsID ts_id;
  common::PageArena pa;

  int ret = meta.init(measurement_name, common::TSDataType::INT32, 100, &stat,
                      ts_id, 1, common::PLAIN, common::UNCOMPRESSED, pa);
  EXPECT_EQ(ret, common::E_OK);
  EXPECT_EQ(meta.data_type_, common::TSDataType::INT32);
  EXPECT_EQ(meta.offset_of_chunk_header_, 100);
  EXPECT_EQ(meta.statistic_, &stat);
  EXPECT_EQ(meta.ts_id_, ts_id);
  EXPECT_EQ(meta.mask_, 1);
}

TEST(ChunkGroupMetaTest, Constructor) {
  common::PageArena pa;
  ChunkGroupMeta group_meta(&pa);
  EXPECT_EQ(group_meta.chunk_meta_list_.size(), 0);
}

TEST(ChunkGroupMetaTest, Init) {
  common::PageArena pa;
  ChunkGroupMeta group_meta(&pa);
  int ret = group_meta.
      init(std::make_shared<StringArrayDeviceID>("device_1"));
  EXPECT_EQ(ret, common::E_OK);
}

TEST(ChunkGroupMetaTest, Push) {
  common::PageArena pa;
  ChunkGroupMeta group_meta(&pa);
  ChunkMeta meta;
  int ret = group_meta.push(&meta);
  EXPECT_EQ(ret, common::E_OK);
  EXPECT_EQ(group_meta.chunk_meta_list_.size(), 1);
}

class TimeseriesIndexTest : public ::testing::Test {
};

TEST_F(TimeseriesIndexTest, ConstructorAndDestructor) {
  TimeseriesIndex tsIndex;
  EXPECT_EQ(tsIndex.get_data_type(), common::INVALID_DATATYPE);
  EXPECT_EQ(tsIndex.get_statistic(), nullptr);
  EXPECT_EQ(tsIndex.get_chunk_meta_list(), nullptr);
}

TEST_F(TimeseriesIndexTest, ResetFunction) {
  TimeseriesIndex tsIndex;
  tsIndex.reset();
  EXPECT_EQ(tsIndex.get_data_type(), common::VECTOR);
  EXPECT_EQ(tsIndex.get_statistic(), nullptr);
  EXPECT_EQ(tsIndex.get_chunk_meta_list(), nullptr);
}

TEST_F(TimeseriesIndexTest, SerializeAndDeserialize) {
  common::PageArena arena;
  arena.init(1024, common::MOD_TIMESERIES_INDEX_OBJ);
  TimeseriesIndex tsIndex;
  common::ByteStream out(1024, common::MOD_TIMESERIES_INDEX_OBJ);
  char name[] = "test_measurement";
  common::String measurementName(name, sizeof(name));
  tsIndex.set_measurement_name(measurementName);
  tsIndex.set_ts_meta_type(1);
  tsIndex.set_data_type(common::TSDataType::INT32);
  tsIndex.init_statistic(common::TSDataType::INT32);
  common::TsID tsID(1, 2, 3);
  tsIndex.set_ts_id(tsID);

  int ret = tsIndex.serialize_to(out);
  EXPECT_EQ(ret, common::E_OK);

  TimeseriesIndex tsIndexDeserialized;
  ret = tsIndexDeserialized.deserialize_from(out, &arena);
  EXPECT_EQ(ret, common::E_OK);
  EXPECT_EQ(tsIndexDeserialized.get_data_type(), common::TSDataType::INT32);
}

class TSMIteratorTest : public ::testing::Test {
protected:
  void SetUp() override {
    arena.init(1024, common::MOD_DEFAULT);
    chunk_group_meta_list_ =
        new common::SimpleList<ChunkGroupMeta *>(&arena);
    void *buf = arena.alloc(sizeof(ChunkGroupMeta));
    auto chunk_group_meta = new(buf) ChunkGroupMeta(&arena);
    chunk_group_meta->device_id_ = std::make_shared<StringArrayDeviceID>(
        "device_1");

    buf = arena.alloc(sizeof(ChunkMeta));
    auto chunk_meta = new(buf) ChunkMeta();
    char measure_name[] = "measurement_1";
    common::String measurement_name(measure_name, sizeof(measure_name));
    stat_ = StatisticFactory::alloc_statistic(common::TSDataType::INT32);
    common::TsID ts_id;
    chunk_meta->init(measurement_name, common::TSDataType::INT32, 100,
                     stat_, ts_id, 1, common::PLAIN, common::UNCOMPRESSED,
                     arena);

    chunk_group_meta->chunk_meta_list_.push_back(chunk_meta);
    chunk_group_meta_list_->push_back(chunk_group_meta);
  }

  void TearDown() override {
    for (auto iter = chunk_group_meta_list_->begin(); iter != chunk_group_meta_list_->end(); iter++) {
      iter.get()->device_id_.reset();
    }
    delete chunk_group_meta_list_;
    StatisticFactory::free(stat_);
  }

  common::PageArena arena;
  Statistic *stat_;
  common::SimpleList<ChunkGroupMeta *> *chunk_group_meta_list_;
};

TEST_F(TSMIteratorTest, InitSuccess) {
  TSMIterator iter(*chunk_group_meta_list_);
  ASSERT_EQ(iter.init(), common::E_OK);
}

TEST_F(TSMIteratorTest, InitEmptyList) {
  common::PageArena arena;
  common::SimpleList<ChunkGroupMeta *> empty_list(&arena);
  TSMIterator iter(empty_list);
  ASSERT_EQ(iter.init(), common::E_OK);
}

TEST_F(TSMIteratorTest, HasNext) {
  TSMIterator iter(*chunk_group_meta_list_);
  iter.init();
  ASSERT_TRUE(iter.has_next());
}

TEST_F(TSMIteratorTest, GetNext) {
  TSMIterator iter(*chunk_group_meta_list_);
  iter.init();

  std::shared_ptr<IDeviceID> ret_device_name;
  common::String ret_measurement_name;
  TimeseriesIndex ret_ts_index;

  ASSERT_TRUE(iter.has_next());
  ASSERT_EQ(
      iter.get_next(ret_device_name, ret_measurement_name, ret_ts_index),
      common::E_OK);
  common::PageArena arena;
  char device_name[] = "device_1";
  auto expect_str = std::make_shared<StringArrayDeviceID>(device_name);

  ASSERT_TRUE(ret_device_name->operator==(*expect_str));

  ASSERT_EQ(
      iter.get_next(ret_device_name, ret_measurement_name, ret_ts_index),
      common::E_NO_MORE_DATA);
}

class MetaIndexEntryTest : public ::testing::Test {
protected:
  common::PageArena pa_;
  common::ByteStream *out_;
  std::shared_ptr<MeasurementMetaIndexEntry> entry_;

  void SetUp() override {
    out_ = new common::ByteStream(1024, common::MOD_DEFAULT);
    entry_ = std::make_shared<MeasurementMetaIndexEntry>();
  }

  void TearDown() override { delete out_; }
};

TEST_F(MetaIndexEntryTest, InitSuccess) {
  std::string name = "test_name";
  int64_t offset = 123456;
  ASSERT_EQ(entry_->init(name, offset, pa_), common::E_OK);
  ASSERT_EQ(entry_->offset_, offset);
}

TEST_F(MetaIndexEntryTest, SerializeDeserialize) {
  std::string name = "test_name";
  int64_t offset = 123456;
  entry_->init(name, offset, pa_);

  ASSERT_EQ(entry_->serialize_to(*out_), common::E_OK);

  MeasurementMetaIndexEntry new_entry;
  ASSERT_EQ(new_entry.deserialize_from(*out_, &pa_), common::E_OK);
  ASSERT_EQ(new_entry.offset_, offset);
}

class MetaIndexNodeTest : public ::testing::Test {
protected:
  common::PageArena pa_;
  common::ByteStream *out_;
  MetaIndexNode node_;

  MetaIndexNodeTest()
      : node_(&pa_) {
  }

  void SetUp() override {
    out_ = new common::ByteStream(1024, common::MOD_DEFAULT);
  }

  void TearDown() override { delete out_; }
};

TEST_F(MetaIndexNodeTest, GetMeasurementFirstChild) {
  ASSERT_EQ(node_.peek(), nullptr);

  auto entry = std::make_shared<MeasurementMetaIndexEntry>();
  entry->init("child_name", 0, pa_);
  node_.push_entry(entry);

  ASSERT_EQ(node_.peek(), entry);
}

TEST_F(MetaIndexNodeTest, GetDeviceFirstChild) {
  ASSERT_EQ(node_.peek(), nullptr);
  auto device_id = std::make_shared<StringArrayDeviceID>("device_1");
  auto entry = std::make_shared<DeviceMetaIndexEntry>(device_id, 0);
  node_.push_entry(entry);

  ASSERT_EQ(node_.peek(), entry);
}

TEST_F(MetaIndexNodeTest, MeasurementSerializeDeserialize) {
  auto entry = std::make_shared<MeasurementMetaIndexEntry>();
  entry->init("child_name", 123, pa_);
  node_.push_entry(entry);
  node_.end_offset_ = 456;
  node_.node_type_ = LEAF_MEASUREMENT;

  ASSERT_EQ(node_.serialize_to(*out_), common::E_OK);

  MetaIndexNode new_node(&pa_);
  ASSERT_EQ(new_node.deserialize_from(*out_), common::E_OK);
  ASSERT_EQ(new_node.end_offset_, 456);
  ASSERT_EQ(new_node.node_type_, LEAF_MEASUREMENT);

  ASSERT_EQ(new_node.peek()->get_name(), entry->get_name());
  ASSERT_EQ(new_node.peek()->get_offset(), entry->get_offset());
}

TEST_F(MetaIndexNodeTest, DeviceSerializeDeserialize) {
  auto device_id = std::make_shared<StringArrayDeviceID>("device_1");
  auto entry = std::make_shared<DeviceMetaIndexEntry>(device_id, 0);
  node_.push_entry(entry);
  node_.end_offset_ = 456;
  node_.node_type_ = LEAF_DEVICE;

  ASSERT_EQ(node_.serialize_to(*out_), common::E_OK);

  MetaIndexNode new_node(&pa_);
  ASSERT_EQ(new_node.device_deserialize_from(*out_), common::E_OK);
  ASSERT_EQ(new_node.end_offset_, 456);
  ASSERT_EQ(new_node.node_type_, LEAF_DEVICE);

  ASSERT_TRUE(new_node.peek()->get_device_id()->operator==(*entry->get_device_id()));
  ASSERT_EQ(new_node.peek()->get_offset(), entry->get_offset());
}

class MetaIndexNodeSearchTest : public ::testing::Test {
protected:
  common::PageArena arena_;
  MetaIndexNode node_;
  std::shared_ptr<MeasurementMetaIndexEntry> entry1_ = std::make_shared<
      MeasurementMetaIndexEntry>();
  std::shared_ptr<MeasurementMetaIndexEntry> entry2_ = std::make_shared<
      MeasurementMetaIndexEntry>();
  std::shared_ptr<MeasurementMetaIndexEntry> entry3_ = std::make_shared<
      MeasurementMetaIndexEntry>();

  MetaIndexNodeSearchTest()
      : node_(&arena_) {
    entry1_->init("apple", 10, arena_);
    entry2_->init("banana", 20, arena_);
    entry3_->init("cherry", 30, arena_);
    node_.children_.push_back(entry1_);
    node_.children_.push_back(entry2_);
    node_.children_.push_back(entry3_);
    node_.end_offset_ = 40;
    node_.pa_ = &arena_;
  }
};

TEST_F(MetaIndexNodeSearchTest, ExactSearchFound) {
  const std::string ret_entry_name("");
  std::shared_ptr<MeasurementMetaIndexEntry> ret_entry = std::make_shared<
      MeasurementMetaIndexEntry>();
  ret_entry->init(ret_entry_name, 0, arena_);
  int64_t ret_offset = 0;
  int result = node_.binary_search_children(
      std::make_shared<StringComparable>("banana"),
      true, *ret_entry, ret_offset);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(ret_offset, 30);
}

TEST_F(MetaIndexNodeSearchTest, ExactSearchNotFound) {
  const std::string ret_entry_name("");
  std::shared_ptr<MeasurementMetaIndexEntry> ret_entry = std::make_shared<
      MeasurementMetaIndexEntry>();
  ret_entry->init(ret_entry_name, 0, arena_);
  int64_t ret_offset = 0;
  char search_name[] = "grape";
  int result = node_.binary_search_children(
      std::make_shared<StringComparable>(search_name),
      true, *ret_entry, ret_offset);
  ASSERT_EQ(result, common::E_NOT_EXIST);
}

TEST_F(MetaIndexNodeSearchTest, NonExactSearchFound) {
  const std::string ret_entry_name("");
  std::shared_ptr<MeasurementMetaIndexEntry> ret_entry = std::make_shared<
      MeasurementMetaIndexEntry>();
  ret_entry->init(ret_entry_name, 0, arena_);
  int64_t ret_offset = 0;
  char search_name[] = "blueberry";
  int result = node_.binary_search_children(
      std::make_shared<StringComparable>(search_name),
      false, *ret_entry, ret_offset);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(ret_offset, 30);
}

TEST_F(MetaIndexNodeSearchTest, NonExactSearchNotFound) {
  const std::string ret_entry_name("");
  std::shared_ptr<MeasurementMetaIndexEntry> ret_entry = std::make_shared<
      MeasurementMetaIndexEntry>();
  ret_entry->init(ret_entry_name, 0, arena_);
  int64_t ret_offset = 0;
  char search_name[] = "aardvark";
  int result = node_.binary_search_children(
      std::make_shared<StringComparable>(search_name),
      false, *ret_entry, ret_offset);
  ASSERT_EQ(result, common::E_NOT_EXIST);
}

class TsFileMetaTest : public ::testing::Test {
protected:
  common::PageArena pa_;
  common::ByteStream *out_;
  TsFileMeta meta_;

  void SetUp() override {
    out_ = new common::ByteStream(1024, common::MOD_DEFAULT);
  }

  void TearDown() override {
    delete out_;
  }
};

TEST_F(TsFileMetaTest, SerializeDeserialize) {
  std::shared_ptr<IDeviceID> device_id = std::make_shared<StringArrayDeviceID>("device");
  auto entry = std::make_shared<DeviceMetaIndexEntry>(device_id, 123);
  auto index_node = std::make_shared<MetaIndexNode>(&pa_);
  index_node->end_offset_ = 123456789;
  index_node->children_.emplace_back(entry);
  index_node->children_.emplace_back(entry);
  std::string table_name = "table_name";
  meta_.table_metadata_index_node_map_.insert(std::make_pair(table_name, index_node));

  std::vector<MeasurementSchema* > column_schemas;
  std::vector<ColumnCategory> column_categories;
  column_categories.emplace_back(ColumnCategory::FIELD);
  column_schemas.emplace_back(new MeasurementSchema());

  auto table_schema = std::make_shared<TableSchema>(table_name, column_schemas, column_categories);

  meta_.table_schemas_.insert(std::make_pair(table_name, table_schema));
  meta_.tsfile_properties_.insert(std::make_pair("key", "value"));

  meta_.meta_offset_ = 456;
  void *buf = pa_.alloc(sizeof(BloomFilter));
  meta_.bloom_filter_ = new(buf) BloomFilter();
  meta_.bloom_filter_->init(0.1, 100);

  meta_.serialize_to(*out_);

  TsFileMeta new_meta(&pa_);
  new_meta.deserialize_from(*out_);
  ASSERT_EQ(new_meta.meta_offset_, 456);
  ASSERT_EQ(new_meta.table_metadata_index_node_map_.size(), 1);
  ASSERT_EQ(new_meta.table_metadata_index_node_map_[table_name]->children_.size(), 2);
  ASSERT_EQ(new_meta.table_schemas_.size(), 1);
  ASSERT_EQ(new_meta.table_schemas_[table_name]->get_column_categories().size(), 1);
}
} // namespace storage
