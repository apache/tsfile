#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Smoke-test an installed wheel, outside the Python source directory."""

from importlib.metadata import version
from pathlib import Path
import tempfile

import tsfile
from tsfile import (
    ColumnCategory,
    ColumnSchema,
    Field,
    RowRecord,
    TableSchema,
    Tablet,
    TimeseriesSchema,
    TSDataType,
    TsFileReader,
    TsFileTableWriter,
    TsFileWriter,
)


def main():
    repository = Path(__file__).resolve().parents[2]
    module = Path(tsfile.__file__).resolve()
    if repository / "python" in module.parents:
        raise RuntimeError(
            f"Imported the source tree instead of the installed wheel: {module}"
        )

    with tempfile.TemporaryDirectory(prefix="tsfile-wheel-smoke-") as temporary:
        table_path = str(Path(temporary) / "table.tsfile")
        schema = TableSchema(
            "sensors",
            [
                ColumnSchema("site", TSDataType.STRING, ColumnCategory.TAG),
                ColumnSchema("value", TSDataType.INT64, ColumnCategory.FIELD),
            ],
        )
        with TsFileTableWriter(table_path, schema) as writer:
            tablet = Tablet(["site", "value"], [TSDataType.STRING, TSDataType.INT64], 3)
            for row, value in enumerate([10, 20, 30]):
                tablet.add_timestamp(row, row)
                tablet.add_value_by_name("site", row, "north")
                tablet.add_value_by_name("value", row, value)
            writer.write_table(tablet)

        with TsFileReader(table_path) as reader:
            with reader.query_table("sensors", ["site", "value"], 0, 2) as result:
                values = []
                while result.next():
                    assert result.get_value_by_name("site") == "north"
                    values.append(result.get_value_by_name("value"))
                assert values == [10, 20, 30], values

        tree_path = str(Path(temporary) / "tree.tsfile")
        with TsFileWriter(tree_path) as writer:
            writer.register_timeseries(
                "root.device", TimeseriesSchema("value", TSDataType.INT64)
            )
            for row, value in enumerate([40, 50]):
                writer.write_row_record(
                    RowRecord(
                        "root.device", row, [Field("value", value, TSDataType.INT64)]
                    )
                )
        with TsFileReader(tree_path) as reader:
            with reader.query_timeseries("root.device", ["value"], 0, 1) as result:
                values = []
                while result.next():
                    values.append(result.get_value_by_index(2))
                assert values == [40, 50], values

    print(
        f"PASS: installed tsfile {version('tsfile')} from {module}; table/tree read-write"
    )


if __name__ == "__main__":
    main()
