/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <tsfile/common/path.h>
#include <tsfile/file/write_file.h>
#include <tsfile/reader/tsfile_reader.h>
#include <tsfile/writer/tsfile_table_writer.h>

#include <cstdio>
#include <vector>

static int round_trip(common::CompressionType compression) {
    const char* path = "pkgconfig-consumer.tsfile";
    {
        storage::WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        if (file.create(path, flags, 0666) != common::E_OK) return 1;
        storage::TableSchema schema(
            "table1", {common::ColumnSchema("value", common::INT64, compression,
                                            common::PLAIN,
                                            common::ColumnCategory::FIELD)});
        storage::TsFileTableWriter writer(&file, &schema);
        storage::Tablet tablet("table1", {"value"}, {common::INT64},
                               {common::ColumnCategory::FIELD}, 16);
        for (int row = 0; row < 16; ++row) {
            tablet.add_timestamp(row, row);
            tablet.add_value(row, "value", static_cast<int64_t>(row * 7));
        }
        if (writer.write_table(tablet) != common::E_OK ||
            writer.close() != common::E_OK)
            return 2;
    }
    storage::TsFileReader reader;
    if (reader.open(path) != common::E_OK) return 3;
    storage::ResultSet* result = nullptr;
    if (reader.query("table1", {"value"}, 0, 100, result) != common::E_OK)
        return 4;
    bool has_next = false;
    int rows = 0;
    int code = common::E_OK;
    while ((code = result->next(has_next)) == common::E_OK && has_next) {
        if (result->get_value<int64_t>(1) != rows ||
            result->get_value<int64_t>(2) != rows * 7)
            return 5;
        ++rows;
    }
    result->close();
    reader.close();
    std::remove(path);
    return code == common::E_OK && rows == 16 ? 0 : 6;
}

int main() {
    storage::libtsfile_init();
    // The inline Path constructor and the out-of-line device parser must
    // agree, including identifiers whose raw bytes are replaced only by CLI
    // output formatting. Different exported macros used to violate this.
    const std::string device_name = "root.device\xff";
    const std::string measurement = "value\xe1\x80";
    storage::StringArrayDeviceID device(device_name);
    storage::Path path(device_name + "." + measurement);
    if (*path.device_id_ != device || path.measurement_ != measurement)
        return 7;
    std::vector<common::CompressionType> codecs = {common::UNCOMPRESSED};
#ifdef ENABLE_SNAPPY
    codecs.push_back(common::SNAPPY);
#endif
#ifdef ENABLE_LZ4
    codecs.push_back(common::LZ4);
#endif
#ifdef ENABLE_LZOKAY
    codecs.push_back(common::LZO);
#endif
#ifdef ENABLE_ZLIB
    codecs.push_back(common::GZIP);
#endif
#ifdef ENABLE_ZSTD
    codecs.push_back(common::ZSTD);
#endif
#ifdef ENABLE_LZMA2
    codecs.push_back(common::LZMA2);
#endif
    for (auto codec : codecs) {
        int code = round_trip(codec);
        if (code != 0) return code;
    }
    return 0;
}
