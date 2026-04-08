# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os
from datetime import date

import numpy as np
import pandas as pd
import pytest

from tsfile import ColumnSchema, TableSchema, TSDataType, ColumnCategory
from tsfile import Tablet
from tsfile import TsFileTableWriter, TsFileReader


def test_batch_read_arrow_basic():
    file_path = "test_batch_arrow_basic.tsfile"
    table = TableSchema(
        "test_table",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value1", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("value2", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
        print("t1")

        with TsFileTableWriter(file_path, table) as writer:
            tablet = Tablet(
                ["device", "value1", "value2"],
                [TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE],
                1000,
            )
            for i in range(1000):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, f"device_{i}")
                tablet.add_value_by_name("value1", i, i * 10)
                tablet.add_value_by_name("value2", i, i * 1.5)
            writer.write_table(tablet)
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("pyarrow is not installed")
        
        reader = TsFileReader(file_path)
        result_set = reader.query_table(
            table_name="test_table",
            column_names=["device", "value1", "value2"],
            start_time=0,
            end_time=1000,
            batch_size=256,
        )
        
        total_rows = 0
        batch_count = 0
        while True:
            table = result_set.read_arrow_batch()
            if table is None:
                break
            
            batch_count += 1
            assert isinstance(table, pa.Table)
            assert len(table) > 0
            total_rows += len(table)

            column_names = table.column_names
            assert "time" in column_names
            assert "device" in column_names
            assert "value1" in column_names
            assert "value2" in column_names
        
        assert total_rows == 1000
        assert batch_count > 0
        
        result_set.close()
        reader.close()
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_batch_read_arrow_compare_with_dataframe():
    file_path = "test_batch_arrow_compare.tsfile"
    table = TableSchema(
        "test_table",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value1", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("value2", TSDataType.FLOAT, ColumnCategory.FIELD),
            ColumnSchema("value3", TSDataType.BOOLEAN, ColumnCategory.FIELD),
        ],
    )
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)

        with TsFileTableWriter(file_path, table) as writer:
            tablet = Tablet(
                ["device", "value1", "value2", "value3"],
                [TSDataType.STRING, TSDataType.INT32, TSDataType.FLOAT, TSDataType.BOOLEAN],
                500,
            )
            for i in range(500):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, f"device_{i}")
                tablet.add_value_by_name("value1", i, i * 2)
                tablet.add_value_by_name("value2", i, i * 1.1)
                tablet.add_value_by_name("value3", i, i % 2 == 0)
            writer.write_table(tablet)
        
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("pyarrow is not installed")

        reader1 = TsFileReader(file_path)
        result_set1 = reader1.query_table(
            table_name="test_table",
            column_names=["device", "value1", "value2", "value3"],
            start_time=0,
            end_time=500,
            batch_size=100,
        )
        
        arrow_tables = []
        while True:
            table = result_set1.read_arrow_batch()
            if table is None:
                break
            arrow_tables.append(table)

        if arrow_tables:
            combined_arrow_table = pa.concat_tables(arrow_tables)
            df_arrow = combined_arrow_table.to_pandas()
        else:
            df_arrow = pd.DataFrame()
        
        result_set1.close()
        reader1.close()
        reader2 = TsFileReader(file_path)
        result_set2 = reader2.query_table(
            table_name="test_table",
            column_names=["device", "value1", "value2", "value3"],
            start_time=0,
            end_time=500,
        )
        
        df_traditional = result_set2.read_data_frame(max_row_num=1000)
        result_set2.close()
        reader2.close()

        assert len(df_arrow) == len(df_traditional)
        assert len(df_arrow) == 500

        for col in ["time", "device", "value1", "value2", "value3"]:
            assert col in df_arrow.columns
            assert col in df_traditional.columns

        df_arrow_sorted = df_arrow.sort_values("time").reset_index(drop=True)
        df_traditional_sorted = df_traditional.sort_values("time").reset_index(drop=True)
        
        for i in range(len(df_arrow_sorted)):
            assert df_arrow_sorted.iloc[i]["time"] == df_traditional_sorted.iloc[i]["time"]
            assert df_arrow_sorted.iloc[i]["device"] == df_traditional_sorted.iloc[i]["device"]
            assert df_arrow_sorted.iloc[i]["value1"] == df_traditional_sorted.iloc[i]["value1"]
            assert abs(df_arrow_sorted.iloc[i]["value2"] - df_traditional_sorted.iloc[i]["value2"]) < 1e-5
            assert df_arrow_sorted.iloc[i]["value3"] == df_traditional_sorted.iloc[i]["value3"]
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_batch_read_arrow_empty_result():
    file_path = "test_batch_arrow_empty.tsfile"
    table = TableSchema(
        "test_table",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.INT64, ColumnCategory.FIELD),
        ],
    )
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)

        with TsFileTableWriter(file_path, table) as writer:
            tablet = Tablet(
                ["device", "value"],
                [TSDataType.STRING, TSDataType.INT64],
                10,
            )
            for i in range(10):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, f"device_{i}")
                tablet.add_value_by_name("value", i, i)
            writer.write_table(tablet)
        
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("pyarrow is not installed")

        reader = TsFileReader(file_path)
        result_set = reader.query_table(
            table_name="test_table",
            column_names=["device", "value"],
            start_time=1000,
            end_time=2000,
            batch_size=100,
        )

        table = result_set.read_arrow_batch()
        assert table is None
        
        result_set.close()
        reader.close()
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_batch_read_arrow_time_range():

    file_path = "test_batch_arrow_time_range.tsfile"
    table = TableSchema(
        "test_table",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.INT64, ColumnCategory.FIELD),
        ],
    )
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)

        with TsFileTableWriter(file_path, table) as writer:
            tablet = Tablet(
                ["device", "value"],
                [TSDataType.STRING, TSDataType.INT64],
                1000,
            )
            for i in range(1000):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, f"device_{i}")
                tablet.add_value_by_name("value", i, i)
            writer.write_table(tablet)
        
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("pyarrow is not installed")

        reader = TsFileReader(file_path)
        result_set = reader.query_table(
            table_name="test_table",
            column_names=["device", "value"],
            start_time=100,
            end_time=199,
            batch_size=50,
        )
        
        total_rows = 0
        while True:
            table = result_set.read_arrow_batch()
            if table is None:
                break
            total_rows += len(table)
            df = table.to_pandas()
            assert df["time"].min() >= 100
            assert df["time"].max() <= 199
        
        assert total_rows == 100
        
        result_set.close()
        reader.close()
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_batch_read_arrow_all_datatypes():
    file_path = "test_batch_arrow_all_datatypes.tsfile"
    table = TableSchema(
        "test_table",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("bool_val", TSDataType.BOOLEAN, ColumnCategory.FIELD),
            ColumnSchema("int32_val", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("int64_val", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("float_val", TSDataType.FLOAT, ColumnCategory.FIELD),
            ColumnSchema("double_val", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("string_val", TSDataType.STRING, ColumnCategory.FIELD),
            ColumnSchema("date_val", TSDataType.DATE, ColumnCategory.FIELD),
        ],
    )
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)

        with TsFileTableWriter(file_path, table) as writer:
            tablet = Tablet(
                ["device", "bool_val", "int32_val", "int64_val", "float_val", "double_val", "string_val", "date_val"],
                [
                    TSDataType.STRING,
                    TSDataType.BOOLEAN,
                    TSDataType.INT32,
                    TSDataType.INT64,
                    TSDataType.FLOAT,
                    TSDataType.DOUBLE,
                    TSDataType.STRING,
                    TSDataType.DATE,
                ],
                200,
            )
            for i in range(200):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, f"device_{i}")
                tablet.add_value_by_name("bool_val", i, i % 2 == 0)
                tablet.add_value_by_name("int32_val", i, i * 2)
                tablet.add_value_by_name("int64_val", i, i * 3)
                tablet.add_value_by_name("float_val", i, i * 1.1)
                tablet.add_value_by_name("double_val", i, i * 2.2)
                tablet.add_value_by_name("string_val", i, f"string_{i}")
                tablet.add_value_by_name("date_val", i, date(2025, 1, (i % 28) + 1))
            writer.write_table(tablet)
        
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("pyarrow is not installed")

        reader = TsFileReader(file_path)
        result_set = reader.query_table(
            table_name="test_table",
            column_names=["device", "bool_val", "int32_val", "int64_val", "float_val", "double_val", "string_val", "date_val"],
            start_time=0,
            end_time=200,
            batch_size=50,
        )
        
        total_rows = 0
        while True:
            table = result_set.read_arrow_batch()
            if table is None:
                break
            
            total_rows += len(table)
            df = table.to_pandas()

            assert "time" in df.columns
            assert "device" in df.columns
            assert "bool_val" in df.columns
            assert "int32_val" in df.columns
            assert "int64_val" in df.columns
            assert "float_val" in df.columns
            assert "double_val" in df.columns
            assert "string_val" in df.columns
            assert "date_val" in df.columns
        
        assert total_rows == 200
        
        result_set.close()
        reader.close()
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_batch_read_arrow_no_pyarrow():
    file_path = "test_batch_arrow_no_pyarrow.tsfile"
    table = TableSchema(
        "test_table",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.INT64, ColumnCategory.FIELD),
        ],
    )
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)

        with TsFileTableWriter(file_path, table) as writer:
            tablet = Tablet(
                ["device", "value"],
                [TSDataType.STRING, TSDataType.INT64],
                10,
            )
            for i in range(10):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, f"device_{i}")
                tablet.add_value_by_name("value", i, i)
            writer.write_table(tablet)
        
        reader = TsFileReader(file_path)
        result_set = reader.query_table(
            table_name="test_table",
            column_names=["device", "value"],
            start_time=0,
            end_time=10,
            batch_size=5,
        )
        result_set.close()
        reader.close()
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    pytest.main([
        "test_batch_arrow.py",
        "-s", "-v"
    ])
