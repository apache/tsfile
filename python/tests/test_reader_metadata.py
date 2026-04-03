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
from tsfile.schema import DeviceID


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
        series = meta_all[device]
        assert len(series) == 1
        m = series[0]
        assert m.measurement_name == "m_int"
        assert m.data_type == TSDataType.INT32
        st = m.statistic
        assert st.has_statistic
        assert st.row_count == 3
        assert st.start_time == 1
        assert st.end_time == 3
        assert st.sum_valid
        assert st.sum == pytest.approx(60.0)

        assert reader.get_timeseries_metadata([]) == {}

        sub = reader.get_timeseries_metadata([DeviceID(device)])
        assert device in sub
        assert len(sub[device]) == 1

        sub_str = reader.get_timeseries_metadata([device])
        assert device in sub_str
    finally:
        reader.close()
        try:
            os.unlink(path)
        except OSError:
            pass
