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
#ifndef WRITER_TSFILE_TABLE_WRITER_H
#define WRITER_TSFILE_TABLE_WRITER_H

#include "writer/tsfile_writer.h"

namespace storage {

/**
 * @brief Facilitates writing structured table data into a TsFile with a
 * specified schema.
 *
 * The TsFileTableWriter class is designed to write structured data,
 * particularly suitable for time-series data, into a file optimized for
 * efficient storage and retrieval (referred to as TsFile here). It allows users
 * to define the schema of the tables they want to write, add rows of data
 * according to that schema, and serialize this data into a TsFile.
 * Additionally, it provides options to limit memory usage during the writing
 * process.
 */
class TsFileTableWriter {
   public:
    /**
     * TsFileTableWriter is used to write table data into a target file with the
     * given schema, optionally limiting the memory usage.
     *
     * @param writer_file Target file where the table data will be written. Must
     * not be null.
     * @param table_schema Used to construct table structures. Defines the
     * schema of the table being written.
     * @param memory_threshold Optional parameter. When the size of written
     * data exceeds this value, the data will be automatically flushed to the
     * disk. Default value is 128MB.
     */
    template <typename T>
    explicit TsFileTableWriter(storage::WriteFile* writer_file, T* table_schema,
                               uint64_t memory_threshold = 128 * 1024 * 1024) {
        static_assert(!std::is_same<T, std::nullptr_t>::value,
                      "table_schema cannot be nullptr");
        tsfile_writer_ = std::make_shared<TsFileWriter>();
        tsfile_writer_->init(writer_file);
        tsfile_writer_->set_generate_table_schema(false);

        // Perform a deep copy. The source TableSchema object may be
        // stack/heap-allocated.
        auto table_schema_ptr = std::make_shared<TableSchema>(*table_schema);
        error_number = tsfile_writer_->register_table(table_schema_ptr);
        exclusive_table_name_ = table_schema->get_table_name();
        common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
    }

    ~TsFileTableWriter();
    /**
     * Registers a table schema with the writer.
     *
     * @param table_schema The schema of the table to be registered. Must not be
     * null.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int register_table(const std::shared_ptr<TableSchema>& table_schema);
    /**
     * Writes the given tablet data into the target file according to the
     * schema.
     *
     * @param tablet The tablet containing the data to be written. Must not be
     * null.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int write_table(Tablet& tablet) const;
    /**
     * Flushes any buffered data to the underlying storage medium, ensuring all
     * data is written out. This method ensures that all pending writes are
     * persisted.
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int flush();
    /**
     * Closes the writer and releases any resources held by it.
     * After calling this method, no further operations should be performed on
     * this instance.
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int close();

   private:
    std::shared_ptr<TsFileWriter> tsfile_writer_;
    // if this TsFile only contains one table, this will be its name, otherwise,
    // it will be an empty string
    std::string exclusive_table_name_;

    // Some errors may not be conveyed during the construction phase, so it's
    // necessary to maintain an internal error code.
    int error_number = common::E_OK;
};

}  // namespace storage

#endif  // WRITER_TSFILE_TABLE_WRITER_H
