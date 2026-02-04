/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace Apache.TsFile.Enums;

/// <summary>
/// Column categories in TsFile v4 table-based model.
/// </summary>
public enum ColumnCategory : byte
{
    /// <summary>
    /// TAG column - used for device identification
    /// </summary>
    Tag = 0,
    
    /// <summary>
    /// FIELD column - measurement values (time series data)
    /// </summary>
    Field = 1,
    
    /// <summary>
    /// TIMESTAMP column - time dimension
    /// </summary>
    Timestamp = 2
}
