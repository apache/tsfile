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

/* Smoke test for lv_tsfile_dump_to_csv. */
#include <stdio.h>

#include "tsfile_labview.h"

int main(int argc, char** argv) {
    setbuf(stdout, NULL);
    const char* in = (argc > 1) ? argv[1] : "lv_dump_input.tsfile";
    const char* out = (argc > 2) ? argv[2] : "dump_out.csv";
    if (argc <= 1) {
        remove(in);
        int write_status = lv_tsfile_write_demo(in, 3);
        if (write_status != 0) {
            printf("write_demo(%s) = %d\n", in, write_status);
            return write_status;
        }
    }
    remove(out);
    int s = lv_tsfile_dump_to_csv(in, out);
    printf("dump_to_csv(%s -> %s) = %d\n", in, out, s);
    if (s != 0) {
        return s;
    }

    FILE* csv = fopen(out, "rb");
    if (csv == NULL) {
        return 1;
    }
    int lines = 0;
    int ch = 0;
    while ((ch = fgetc(csv)) != EOF) {
        if (ch == '\n') {
            ++lines;
        }
    }
    fclose(csv);
    return lines == 4 ? 0 : 1;
}
