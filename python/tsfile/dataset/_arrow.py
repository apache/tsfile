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

"""Internal Arrow conversion helpers for dataset readers."""

import numpy as np


def arrow_column_to_float64(column):
    """Convert a numeric-compatible Arrow column and preserve its nulls."""
    values = np.asarray(
        column.to_numpy(zero_copy_only=False),
        dtype=np.float64,
    )
    if column.null_count:
        # Arrow timestamps become NumPy NaT, whose float64 representation is
        # INT64_MIN. Reapply Arrow's authoritative null mask for every type.
        values = values.copy()
        values[column.is_null().to_numpy(zero_copy_only=False)] = np.nan
    return values
