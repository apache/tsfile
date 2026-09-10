/*
 * Read-only smoke test: opens an existing .tsfile (path from argv[1], default
 * lv_smoke.tsfile), queries lv_table, and prints the rows. Used to isolate
 * whether a write or a read is at fault across bitness/toolchain combos.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tsfile_labview.h"

int main(int argc, char** argv) {
    setbuf(stdout, NULL);
    const char* path = (argc > 1) ? argv[1] : "lv_smoke.tsfile";
    const char* table = "lv_table";

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
    return 0;
}
