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
import math
import os

import numpy as np

import tsfile as ts
from tsfile import TIME_COLUMN


def test_load_tsfile_from_iotdb():
    test_path = os.path.dirname(os.path.abspath(__file__))
    dir_path = os.path.join(test_path, "resources")
    simple_tree_path = os.path.join(dir_path, "simple_tree.tsfile")
    df = ts.to_dataframe(simple_tree_path)

    ## --------
    assert len(df) == 105, "row count mismatch"
    assert df[TIME_COLUMN].isna().sum() == 0
    assert int(df[TIME_COLUMN].sum()) == 15960
    assert df["temperature"].isna().sum() == 5
    assert df["status"].isna().sum() == 5
    assert (df["status"] == True).sum() == 50
    assert (df["status"] == False).sum() == 50
    ## ---------

    #
    simple_tabl1_path = os.path.join(dir_path, "simple_table_t1.tsfile")
    df = ts.to_dataframe(simple_tabl1_path)
    ## ---------
    assert len(df) == 60
    assert df[TIME_COLUMN].isna().sum() == 0
    assert df[TIME_COLUMN].sum() == (
        (1760106020000 + 1760106049000) * 30 // 2
        + (1760106080000 + 1760106109000) * 30 // 2
    )
    assert df["s0"].isna().sum() == 0
    df_s0 = df["s0"]
    assert df["s1"].isna().sum() == 0
    assert df["s2"].isna().sum() == 8
    assert df["s3"].isna().sum() == 0
    assert df["s4"].isna().sum() == 0
    assert df["s4"].nunique() == 60
    assert df["s5"].isna().sum() == 0

    assert df["s5"].sum() == ((1010 + 1039) * 30 // 2 + (1070 + 1099) * 30 // 2)
    assert df["s6"].isna().sum() == 8

    assert df["s6"].sum(skipna=True) == (
        (20 + 49) * 30 // 2
        - (26 + 33 + 39 + 46)
        + (80 + 109) * 30 // 2
        - (86 + 93 + 99 + 106)
    )
    assert df["s7"].isna().sum() == 0
    assert df["s8"].isna().sum() == 0
    assert df["s8"].nunique() == 60
    assert df["s9"].isna().sum() == 8

    df = ts.to_dataframe(simple_tabl1_path, table_name="test", column_names=["s0"])
    assert len(df) == 60
    assert len(df.columns) == 2
    assert df["s0"].equals(df_s0)

    ## ---------

    simple_tabl2_path = os.path.join(dir_path, "simple_table_t2.tsfile")
    df = ts.to_dataframe(simple_tabl2_path)
    ## ---------
    assert len(df) == 40
    assert df[TIME_COLUMN].isna().sum() == 0
    assert int(df[TIME_COLUMN].sum()) == 70404242080000

    assert df["s0"].isna().sum() == 0
    assert df["s1"].isna().sum() == 0
    assert df["s0"].nunique() == 2
    assert df["s1"].nunique() == 2

    assert df["s2"].isna().sum() == 5
    assert int(df["s2"].sum(skipna=True)) == 36450

    assert df["s3"].isna().sum() == 0
    assert np.isclose(float(df["s3"].sum()), 208.0, rtol=1e-6, atol=1e-6)

    assert df["s4"].isna().sum() == 0
    assert df["s4"].nunique() == 40

    assert df["s5"].isna().sum() == 0
    assert int(df["s5"].sum()) == 41680

    assert df["s6"].isna().sum() == 5
    assert int(df["s6"].sum(skipna=True)) == 1800

    assert df["s7"].isna().sum() == 0
    assert np.isclose(float(df["s7"].sum()), 568.0, rtol=1e-6, atol=1e-6)

    assert df["s8"].isna().sum() == 0
    assert df["s8"].nunique() == 40

    assert df["s9"].isna().sum() == 5
    ## ---------
    table_with_time_column_path = os.path.join(
        dir_path, "table_with_time_column.tsfile"
    )

    df = ts.to_dataframe(table_with_time_column_path)
    assert list(df.columns)[0] == "id"
    assert len(df) == 25
    assert math.isclose(df["temperature"].sum(), 2.5, rel_tol=1e-9)
    assert math.isclose(df["humidity"].sum(), 2.5, rel_tol=1e-9)
    assert (df["region_id"] == "loc").sum() == 25
    df_id = df["id"]

    df = ts.to_dataframe(
        table_with_time_column_path,
        table_name="table2",
        column_names=["region_id", "temperature", "humidity"],
    )
    assert list(df.columns)[0] == "id"
    assert len(df) == 25
    assert math.isclose(df["temperature"].sum(), 2.5, rel_tol=1e-9)
    assert (df["region_id"] == "loc").sum() == 25

    df = ts.to_dataframe(
        table_with_time_column_path,
        table_name="table2",
        column_names=["id", "temperature", "humidity"],
    )
    assert list(df.columns)[0] == "time"
    assert df["id"].equals(df["time"])
    assert len(df) == 25
    assert math.isclose(df["temperature"].sum(), 2.5, rel_tol=1e-9)
    assert math.isclose(df["humidity"].sum(), 2.5, rel_tol=1e-9)

    df = ts.to_dataframe(
        table_with_time_column_path, table_name="table2", column_names=["id"]
    )
    assert len(df.columns) == 2
    assert df_id.equals(df["id"])
