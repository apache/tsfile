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

import pandas as pd

from tsfile import ColumnCategory, ColumnSchema, TSDataType, TableSchema, TsFileTableWriter
from tsfile import AlignedTimeseries, Timeseries, TsFileDataFrame


def _write_weather_file(path, start):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("humidity", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(
        {
            "time": [start, start + 1, start + 2],
            "device": ["device_a", "device_a", "device_a"],
            "temperature": [20.0, 21.5, 23.0],
            "humidity": [50.0, 52.0, 55.0],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def test_dataset_top_level_imports():
    assert TsFileDataFrame.__module__ == "tsfile.dataset.dataframe"
    assert Timeseries.__module__ == "tsfile.dataset.timeseries"
    assert AlignedTimeseries.__module__ == "tsfile.dataset.timeseries"


def test_dataset_basic_access_patterns(tmp_path, capsys):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_file(path2, 3)

    with TsFileDataFrame([str(path1), str(path2)], show_progress=False) as tsdf:
        assert len(tsdf) == 2

        first = tsdf[0]
        assert isinstance(first, Timeseries)
        assert first.name in tsdf.list_timeseries()
        assert len(first) == 6
        assert first[0] == 20.0
        assert first[-1] == 23.0

        by_name = tsdf[first.name]
        assert isinstance(by_name, Timeseries)
        assert by_name.name == first.name

        subset = tsdf[:1]
        assert isinstance(subset, TsFileDataFrame)
        assert len(subset) == 1

        selected = tsdf[[0, 1]]
        assert isinstance(selected, TsFileDataFrame)
        assert len(selected) == 2

        aligned = tsdf.loc[0:5, [0, 1]]
        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.shape == (6, 2)

        metadata = tsdf.metadata()
        assert list(metadata["field"]) == ["temperature", "humidity"]

        assert "TsFileDataFrame(2 time series, 2 files)" in repr(tsdf)
        aligned.show(2)
        assert "AlignedTimeseries(6 rows, 2 series)" in capsys.readouterr().out
