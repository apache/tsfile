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

from tsfile import TableSchema, Tablet, TableNotExistError
from tsfile import TsFileWriter


class TsFileTableWriter:
    """
    Facilitates writing structured table data into a TsFile with a specified schema.

    The TsFileTableWriter class is designed to write structured data,
    particularly suitable for time-series data, into a file optimized for
    efficient storage and retrieval (referred to as TsFile here). It allows users
    to define the schema of the tables they want to write, add rows of data
    according to that schema, and serialize this data into a TsFile.
    """

    def __init__(self, path: str, table_schema: TableSchema, memory_threshold = 128 * 1024 * 1024):
        """
        :param path: The path of tsfile, will create if it doesn't exist.
        :param table_schema: describes the schema of the tables they want to write.
        :param memory_threshold(Byte): memory usage threshold for flushing data.
        """
        self.writer = TsFileWriter(path, memory_threshold)
        self.writer.register_table(table_schema)
        self.exclusive_table_name_ = table_schema.get_table_name()

    def write_table(self, tablet: Tablet):
        """
        Write a tablet into table in tsfile.
        :param tablet: stored batch data of a table.
        :return: no return value.
        :raise: TableNotExistError if table does not exist or tablet's table_name does not match tableschema.
        """
        if tablet.get_target_name() is None:
            tablet.set_table_name(self.exclusive_table_name_)
        elif self.exclusive_table_name_ is not None and tablet.get_target_name() != self.exclusive_table_name_:
            raise TableNotExistError
        self.writer.write_table(tablet)

    def close(self):
        """
        Close TsFileTableWriter and will flush data automatically.
        :return: no return value.
        """
        self.writer.close()

    def flush(self):
        """
        Flush current data to tsfile.
        :return: no return value.
        """
        self.writer.flush()

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
