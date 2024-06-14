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
import shutil

import unittest as ut
import numpy as np
import pandas as pd


import tsfile as ts

TABLE_NAME = "test_table"
DATA_PATH = os.path.join(os.path.dirname(__file__), "data")


# test writing data
def test_write_tsfile():
    # test write empty data
    df = pd.DataFrame()
    ts.write_tsfile(DATA_PATH + "/empty.tsfile", TABLE_NAME, df)
    assert not os.path.exists(DATA_PATH + "/empty.tsfile")

    # data without Time
    # 1000 rows data
    level = np.linspace(2000, 3000, num=1000, dtype=np.float32)
    num = np.arange(10000, 11000, dtype=np.int64)
    df = pd.DataFrame({"level": level, "num": num})
    with ut.TestCase().assertRaises(AttributeError):
        ts.write_tsfile(DATA_PATH + "/no_time.tsfile", TABLE_NAME, df)

    # time with wrong type
    time = np.arange(1, 1001, dtype=np.float32)
    df = pd.DataFrame({"Time": time, "level": level, "num": num})
    with ut.TestCase().assertRaises(TypeError):
        ts.write_tsfile(DATA_PATH + "/wrong_time_type.tsfile", TABLE_NAME, df)\
        
    # TXT is not support yet
    time = np.arange(1, 1001, dtype=np.int64)
    text = np.random.choice(["a", "b", "c"], 1000)
    df = pd.DataFrame({"Time": time, "text": text})
    with ut.TestCase().assertRaises(TypeError):
        ts.write_tsfile(DATA_PATH + "/txt.tsfile", TABLE_NAME, df)

    # full datatypes test
    time = np.arange(1, 1001, dtype=np.int64) # int64
    level = np.linspace(2000, 3000, num=1000, dtype=np.float32) # float32
    num = np.arange(10000, 11000, dtype=np.int64) # int64
    bools = np.random.choice([True, False], 1000) # bool
    double = np.random.rand(1000) # double
    df = pd.DataFrame({"Time": time, "level": level, "num": num, "bools": bools, "double": double})
    ts.write_tsfile(DATA_PATH + "/full_datatypes.tsfile", TABLE_NAME, df)

# test reading data
def test_read_tsfile():
    # test read not exist file
    with ut.TestCase().assertRaises(FileNotFoundError):
        ts.read_tsfile(DATA_PATH + "/notexit.tsfile", TABLE_NAME, ["level", "num"])

    # test read empty file
    with open(DATA_PATH + "/empty.tsfile", "w", encoding="utf-8") as f:
        pass

    with ut.TestCase().assertRaises(ValueError):
        ts.read_tsfile(DATA_PATH + "/empty.tsfile", TABLE_NAME, ["level", "num"])
    
    FILE_NAME= DATA_PATH + "/full_datatypes.tsfile"
    # test read data
    ## 1. read all data
    df = ts.read_tsfile(FILE_NAME, TABLE_NAME, ["level", "num", "bools", "double"])
    assert df.shape == (1000, 5)
    assert df["level"].dtype == np.float32
    assert df["Time"].dtype == np.int64
    assert df["num"].dtype == np.int64
    assert df["bools"].dtype == np.bool_
    assert df["double"].dtype == np.float64


    ## 2. read with chunksize
    df = ts.read_tsfile(FILE_NAME, TABLE_NAME, ["level", "num"], chunksize = 100)
    assert df.shape == (100, 3)
    assert df["level"].dtype == np.float32
    assert df["Time"].sum() == np.arange(1, 101).sum()


    ## 3. read with iterator
    chunk_num = 0
    with ts.read_tsfile(FILE_NAME, TABLE_NAME, ["level", "num"], iterator=True, chunksize=100) as reader:
        for chunk in reader:
            assert chunk.shape == (100, 3)
            assert chunk["level"].dtype == np.float32
            assert chunk["Time"].sum() == np.arange(1 + chunk_num *100, 101 + chunk_num * 100).sum()
            chunk_num += 1
    assert chunk_num == 10


    ## 4. read with time scale
    df = ts.read_tsfile(FILE_NAME, TABLE_NAME, ["num"], start_time=50, end_time=99)
    assert df.shape == (50, 2)
    assert df["num"][0] == 10049
    assert df["num"][9] == 10058

    ## 5. read with time scale and chunksize
    df = ts.read_tsfile(FILE_NAME, TABLE_NAME, ["num"], start_time=50, end_time=99, chunksize=10)
    assert df.shape == (10, 2)
    assert df["num"][0] == 10049
    assert df["num"][9] == 10058

    ## 6. read with time scale and iterator
    chunk_num = 0
    with ts.read_tsfile(FILE_NAME, TABLE_NAME, ["num"], start_time=50, end_time=99, iterator=True, chunksize=10) as reader:
        for chunk in reader:
            assert chunk.shape == (10, 2)
            assert chunk["num"][0] == 10049 + chunk_num * 10
            assert chunk["num"][9] == 10058 + chunk_num * 10
            chunk_num += 1
    assert chunk_num == 5

if __name__ == '__main__':
    if os.path.exists(DATA_PATH):
        print("Remove old data")
        shutil.rmtree(DATA_PATH)
        os.makedirs(DATA_PATH)
    else:
        os.makedirs(DATA_PATH)
    test_write_tsfile()
    test_read_tsfile()
    print("All tests passed")
    shutil.rmtree(DATA_PATH)