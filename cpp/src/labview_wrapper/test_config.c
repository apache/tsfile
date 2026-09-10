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

#include <stdio.h>

#include "cwrapper/tsfile_cwrapper.h"
#include "tsfile_labview.h"

#define CHECK(condition)                                           \
    do {                                                           \
        if (!(condition)) {                                        \
            printf("FAIL: %s at line %d\n", #condition, __LINE__); \
            return 1;                                              \
        }                                                          \
    } while (0)

int main(void) {
    const char* path = "lv_config_order.tsfile";
    LV_Handle builder = 0;
    LV_Handle writer = 0;

    remove(path);
    CHECK(lv_tsfile_set_datatype_encoding(LV_TYPE_DOUBLE, TS_ENCODING_PLAIN) ==
          0);
    CHECK(lv_tsfile_set_global_compression(TS_COMPRESSION_UNCOMPRESSED) == 0);
    CHECK(get_datatype_encoding(TS_DATATYPE_DOUBLE) == TS_ENCODING_PLAIN);
    CHECK(get_global_compression() == TS_COMPRESSION_UNCOMPRESSED);

    builder = lv_tsfile_schema_builder_new("signals");
    CHECK(builder != 0);
    CHECK(lv_tsfile_schema_builder_add_column(builder, "value", LV_TYPE_DOUBLE,
                                              LV_CAT_FIELD) == 0);
    CHECK(lv_tsfile_writer_open(path, builder, 0, &writer) == 0);
    CHECK(writer != 0);
    CHECK(lv_tsfile_writer_close(writer) == 0);
    lv_tsfile_schema_builder_free(builder);

    CHECK(get_datatype_encoding(TS_DATATYPE_DOUBLE) == TS_ENCODING_PLAIN);
    CHECK(get_global_compression() == TS_COMPRESSION_UNCOMPRESSED);
    remove(path);
    return 0;
}
