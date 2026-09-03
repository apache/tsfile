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

// DataType identifies the data type of a time series measurement.
type DataType int32

const (
	DataTypeBoolean   DataType = 0
	DataTypeInt32     DataType = 1
	DataTypeInt64     DataType = 2
	DataTypeFloat     DataType = 3
	DataTypeDouble    DataType = 4
	DataTypeText      DataType = 5
	DataTypeVector    DataType = 6
	DataTypeTimestamp DataType = 8
	DataTypeDate      DataType = 9
	DataTypeBlob      DataType = 10
	DataTypeString    DataType = 11
	DataTypeNull      DataType = 254
	DataTypeInvalid   DataType = 255
)

// Encoding identifies the encoding scheme of a chunk.
type Encoding int32

const (
	EncodingPlain      Encoding = 0
	EncodingDictionary Encoding = 1
	EncodingRLE        Encoding = 2
	EncodingDiff       Encoding = 3
	EncodingTS2Diff    Encoding = 4
	EncodingBitmap     Encoding = 5
	EncodingGorillaV1  Encoding = 6
	EncodingRegular    Encoding = 7
	EncodingGorilla    Encoding = 8
	EncodingZigZag     Encoding = 9
	EncodingFreq       Encoding = 10
	EncodingChimp      Encoding = 11
	EncodingSprintz    Encoding = 12
	EncodingRLBE       Encoding = 13
	EncodingCamel      Encoding = 14
	EncodingInvalid    Encoding = 255
)

// Compression identifies the compression algorithm of a chunk.
type Compression int32

const (
	CompressionUncompressed Compression = 0
	CompressionSnappy       Compression = 1
	CompressionGzip         Compression = 2
	CompressionLZO          Compression = 3
	CompressionSDT          Compression = 4
	CompressionPAA          Compression = 5
	CompressionPLA          Compression = 6
	CompressionLZ4          Compression = 7
	CompressionZstd         Compression = 8
	CompressionLZMA2        Compression = 9
	CompressionInvalid      Compression = 255
)

// ColumnCategory identifies the category of a schema entry.
type ColumnCategory int32

const (
	ColumnCategoryTag       ColumnCategory = 0
	ColumnCategoryField     ColumnCategory = 1
	ColumnCategoryAttribute ColumnCategory = 2
	ColumnCategoryTime      ColumnCategory = 3
)
