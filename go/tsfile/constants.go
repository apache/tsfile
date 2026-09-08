// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tsfile

// DataType identifies the data type of a table column.
type DataType int32

const (
	DataTypeBoolean   DataType = 0
	DataTypeInt32     DataType = 1
	DataTypeInt64     DataType = 2
	DataTypeFloat     DataType = 3
	DataTypeDouble    DataType = 4
	DataTypeText      DataType = 5
	DataTypeTimestamp DataType = 8
	DataTypeDate      DataType = 9
	DataTypeBlob      DataType = 10
	DataTypeString    DataType = 11
)

// ColumnCategory identifies the category of a schema entry.
type ColumnCategory int32

const (
	ColumnCategoryTag   ColumnCategory = 0
	ColumnCategoryField ColumnCategory = 1
)
