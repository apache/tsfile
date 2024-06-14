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
import pandas as pd
import os

import tsfile as ts


# test writing data
data_dir = os.path.join(os.path.dirname(__file__), "test.tsfile")
TABLE_NAME = "test_table"

# 1000 rows data
time = np.arange(1, 1001, dtype=np.int64)
level = np.linspace(2000, 3000, num=1000, dtype=np.float32)
num = np.arange(10000, 11000, dtype=np.int64)
df = pd.DataFrame({"Time": time, "level": level, "num": num})

if os.path.exists(data_dir):
    os.remove(data_dir)
ts.write_tsfile(data_dir, TABLE_NAME, df)


# read data we already wrote
# with 20 chunksize
tsfile_ret = ts.read_tsfile(data_dir, TABLE_NAME, ["level", "num"], chunksize=20)
print(tsfile_ret.shape)

# # with 100 chunksize
tsfile_ret = ts.read_tsfile(data_dir, TABLE_NAME, ["level", "num"], chunksize = 100)
print(tsfile_ret.shape)

# # get all data
tsfile_ret = ts.read_tsfile(data_dir, TABLE_NAME, ["level", "num"])
print(tsfile_ret.shape)

# # with iterator
with ts.read_tsfile(data_dir, TABLE_NAME, ["level", "num"], iterator=True, chunksize=100) as reader:
    for chunk in reader:
        print(chunk.shape)

# # with time scale and chunksize
tsfile_ret = ts.read_tsfile(data_dir, TABLE_NAME,
                             ["level"], start_time=50, end_time=100, chunksize=10)
print(tsfile_ret.shape)

# with time scale
tsfile_ret = ts.read_tsfile(data_dir, TABLE_NAME, ["num"], start_time=50, end_time=100)
print(tsfile_ret.shape)


with ts.read_tsfile(
    data_dir,
    TABLE_NAME,
    ["level", "num"],
    iterator=True,
    start_time=100,
    end_time=500,
    chunksize=100,
) as reader:
    for chunk in reader:
        print(chunk.shape)
