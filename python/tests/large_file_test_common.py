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
import random
import string

TARGET_FILE_SIZE = 4 * 1024 * 1024 * 1024
MIN_ACCEPTABLE_FILE_SIZE = 3800 * 1024 * 1024
START_TIME = 1622505600000
TABLET_ROWS = 50000
FLUSH_ROWS = 1_000_000


def get_file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def random_suffix(length: int = 10) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))
