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
import tempfile

import pytest

from tsfile import Field, RowRecord, TimeseriesSchema, TsFileReader, TsFileWriter
from tsfile import TSDataType
from tsfile.schema import (
    BoolTimeseriesStatistic,
    DeviceID,
    IntTimeseriesStatistic,
    StringTimeseriesStatistic,
)


def test_get_all_devices_segments():
    path = os.path.join(tempfile.gettempdir(), "py_reader_metadata_details.tsfile")
    try:
        os.unlink(path)
    except OSError:
        pass

    device = "root.sg.py_details"
    writer = TsFileWriter(path)
    writer.register_timeseries(
        device, TimeseriesSchema("m", TSDataType.INT32))
    writer.write_row_record(
        RowRecord(device, 1, [Field("m", 1, TSDataType.INT32)]))
    writer.close()

    reader = TsFileReader(path)
    try:
        details = reader.get_all_devices()
        assert len(details) == 1
        d0 = details[0]
        assert d0.path == device
        assert d0.table_name == "root.sg"
        assert d0.segments == ("root.sg", "py_details")

        grp = reader.get_timeseries_metadata(None)[device]
        assert grp.table_name == "root.sg"
        assert grp.segments == ("root.sg", "py_details")
        assert len(grp.timeseries) == 1
    finally:
        reader.close()
        try:
            os.unlink(path)
        except OSError:
            pass


def test_get_all_devices_and_timeseries_metadata_statistic():
    path = os.path.join(tempfile.gettempdir(), "py_reader_metadata_stat.tsfile")
    try:
        os.unlink(path)
    except OSError:
        pass

    device = "root.sg.py_meta"
    writer = TsFileWriter(path)
    writer.register_timeseries(
        device, TimeseriesSchema("m_int", TSDataType.INT32))
    for row in range(3):
        v = (row + 1) * 10
        writer.write_row_record(
            RowRecord(
                device,
                row + 1,
                [Field("m_int", v, TSDataType.INT32)],
            )
        )
    writer.close()

    reader = TsFileReader(path)
    try:
        devices = reader.get_all_devices()
        assert len(devices) == 1
        assert devices[0].path == device

        meta_all = reader.get_timeseries_metadata(None)
        assert list(meta_all.keys()) == [device]
        grp = meta_all[device]
        assert grp.table_name == "root.sg"
        assert grp.segments == ("root.sg", "py_meta")
        series = grp.timeseries
        assert len(series) == 1
        m = series[0]
        assert m.measurement_name == "m_int"
        assert m.data_type == TSDataType.INT32
        st = m.statistic
        assert isinstance(st, IntTimeseriesStatistic)
        assert st.has_statistic
        assert st.row_count == 3
        assert st.start_time == 1
        assert st.end_time == 3
        assert st.sum == pytest.approx(60.0)
        assert st.min_int64 == 10
        assert st.max_int64 == 30
        assert st.first_int64 == 10
        assert st.last_int64 == 30

        assert reader.get_timeseries_metadata([]) == {}

        sub = reader.get_timeseries_metadata([DeviceID(device, None, ())])
        assert device in sub
        assert len(sub[device].timeseries) == 1

        sub_str = reader.get_timeseries_metadata([device])
        assert device in sub_str
    finally:
        reader.close()
        try:
            os.unlink(path)
        except OSError:
            pass


def test_get_timeseries_metadata_boolean_statistic():
    path = os.path.join(tempfile.gettempdir(), "py_reader_metadata_bool.tsfile")
    try:
        os.unlink(path)
    except OSError:
        pass

    device = "root.sg.py_bool"
    writer = TsFileWriter(path)
    writer.register_timeseries(
        device, TimeseriesSchema("m_b", TSDataType.BOOLEAN))
    for row, b in enumerate([True, False, True]):
        writer.write_row_record(
            RowRecord(
                device,
                row + 1,
                [Field("m_b", b, TSDataType.BOOLEAN)],
            )
        )
    writer.close()

    reader = TsFileReader(path)
    try:
        meta_all = reader.get_timeseries_metadata(None)
        st = meta_all[device].timeseries[0].statistic
        assert isinstance(st, BoolTimeseriesStatistic)
        assert st.has_statistic
        assert st.sum == pytest.approx(2.0)
        assert st.first_bool is True
        assert st.last_bool is True
    finally:
        reader.close()
        try:
            os.unlink(path)
        except OSError:
            pass


def test_get_timeseries_metadata_string_statistic():
    path = os.path.join(tempfile.gettempdir(), "py_reader_metadata_str.tsfile")
    try:
        os.unlink(path)
    except OSError:
        pass

    device = "root.sg.py_str"
    writer = TsFileWriter(path)
    writer.register_timeseries(
        device, TimeseriesSchema("m_str", TSDataType.STRING))
    for row, s in enumerate(["aa", "cc", "bb"]):
        writer.write_row_record(
            RowRecord(
                device,
                row + 1,
                [Field("m_str", s, TSDataType.STRING)],
            )
        )
    writer.close()

    reader = TsFileReader(path)
    try:
        meta_all = reader.get_timeseries_metadata(None)
        m = meta_all[device].timeseries[0]
        assert m.measurement_name == "m_str"
        assert m.data_type == TSDataType.STRING
        st = m.statistic
        assert isinstance(st, StringTimeseriesStatistic)
        assert st.has_statistic
        assert st.str_min == "aa"
        assert st.str_max == "cc"
        assert st.str_first == "aa"
        assert st.str_last == "bb"
    finally:
        reader.close()
        try:
            os.unlink(path)
        except OSError:
            pass
