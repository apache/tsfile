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

/*
 * Read-only smoke test: with an argv path, opens that existing lv_table file.
 * Without argv, creates an isolated demo fixture before exercising only the
 * reader API. This keeps parallel CTest execution self-contained.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tsfile_labview.h"

int main(int argc, char** argv) {
    setbuf(stdout, NULL);
    const int owns_fixture = argc <= 1;
    const char* path = owns_fixture ? "lv_read_only.tsfile" : argv[1];
    const char* table = owns_fixture ? "demo" : "lv_table";

    if (owns_fixture) {
        remove(path);
        LV_Status fixture_status = lv_tsfile_write_demo(path, 5);
        if (fixture_status != 0) {
            printf("fixture write FAIL %d\n", (int)fixture_status);
            return 1;
        }
    }

    LV_Handle reader = 0;
    LV_Status s = lv_tsfile_reader_open(path, &reader);
    if (s != 0) {
        printf("reader_open FAIL %d\n", (int)s);
        return 1;
    }

    LV_Handle rs = 0;
    s = lv_tsfile_query_table(reader, table, "device\ntemp\ncnt", 0, 100, &rs);
    if (s != 0) {
        printf("query FAIL %d\n", (int)s);
        return 1;
    }

    int32_t ncols = lv_tsfile_rs_column_count(rs);
    printf("column_count = %d\n", ncols);

    int rows = 0;
    LV_Status err = 0;
    while (lv_tsfile_rs_next(rs, &err) == 1 && err == 0) {
        int64_t ts = lv_tsfile_rs_get_i64(rs, 0);
        printf("  ts=%lld | %d cols\n", (long long)ts, ncols);
        ++rows;
    }
    printf("read %d rows (err=%d)\n", rows, (int)err);

    lv_tsfile_rs_free(rs);
    lv_tsfile_reader_close(reader);
    if (owns_fixture) {
        remove(path);
    }
    return 0;
}
