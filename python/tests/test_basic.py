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
import numpy as np
import pytest
from tsfile import schema, Field
from tsfile import Tablet
from tsfile.constants import *


def test_tablet():
    column_names = ["temp1", "temp2", "value1", "value2"]
    data_types = [TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE]
    tablet = Tablet(column_names, data_types)
    tablet.set_table_name("test")

    assert "test" == tablet.get_target_name()
    assert 4 == len(tablet.get_column_name_list())
    assert TSDataType.INT32 == tablet.get_data_type_list()[0]

    tablet.add_timestamp(0, 10)
    tablet.add_value_by_name("temp1", 0, 10)
    tablet.add_value_by_name("temp2", 0, 100)
    tablet.add_value_by_index(2, 0, 0.1)
    tablet.add_value_by_index(3, 0, 0.1)
    # Illegal column name
    with pytest.raises(ValueError):
        tablet.add_value_by_name("temp3", 0, 10)
    # Illegal exists column index
    with pytest.raises(IndexError):
        tablet.add_value_by_index(4, 0, 10)
    # Illegal row index
    with pytest.raises(IndexError):
        tablet.add_value_by_name("temp1", 2048, 10)
    # Illegal data type
    with pytest.raises(TypeError):
        tablet.add_value_by_name("temp1", 2, 10.0)

    # Illegal data scope
    with pytest.raises(OverflowError):
        tablet.add_value_by_index(0, 20, np.iinfo(np.int64).max)

    tablet.add_value_by_index(0, 30, np.iinfo(np.int32).max)

    assert 0.1 == tablet.get_value_list_by_name("value1")[0]
    assert np.iinfo(np.int32).max == tablet.get_value_list_by_name("temp1")[30]

def test_field():
    field_int32 = Field("int32",10, TSDataType.INT32)
    field_int64 = Field("int64", np.iinfo(np.int32).max + 1, TSDataType.INT64)
    field_float = Field("float",10.0, TSDataType.FLOAT)
    field_double = Field("double",10.0, TSDataType.DOUBLE)
    field_bool = Field("bool",True, TSDataType.BOOLEAN)
    field = Field("t",100)

    assert 100 == field.get_value()
    assert np.int64(10) == field_int32.get_long_value()
    assert True == field_bool.get_bool_value()
    assert np.float64(10) == field_double.get_double_value()
    assert np.float32(10) == field_float.get_float_value()
    assert TSDataType.INT64 == field_int64.get_data_type()
    assert False == field_int32.is_null()
    field.set_value(200)
    field.set_data_type(TSDataType.DOUBLE)
    assert np.float32(200) == field.get_float_value()
    assert np.float64(200) == field.get_double_value()
    assert field_int64.get_value() == field_int64.get_timestamp_value()

    field = Field("t", "t1", TSDataType.STRING)
    assert "t1" == field.get_string_value()
    assert "10" == field_int32.get_string_value()

    with pytest.raises(OverflowError):
        field_int64.get_int_value()





