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

import (
	"fmt"
	"testing"
)

// The expected values below are pinned to the C ABI definitions in
// cpp/src/cwrapper/tsfile_cwrapper.h (TSDataType, TSEncoding,
// CompressionType, ColumnCategory) and cpp/src/cwrapper/errno_define_c.h
// (RET_*). Any change in either header must be mirrored here.

// cErrnoCodes lists every RET_* value defined in errno_define_c.h.
var cErrnoCodes = map[int32]string{
	0:  "RET_OK",
	1:  "RET_OOM",
	2:  "RET_NOT_EXIST",
	3:  "RET_ALREADY_EXIST",
	4:  "RET_INVALID_ARG",
	5:  "RET_OUT_OF_RANGE",
	6:  "RET_PARTIAL_READ",
	8:  "RET_INVALID_SCHEMA",
	20: "RET_OVERFLOW",
	21: "RET_NO_MORE_DATA",
	22: "RET_OUT_OF_ORDER",
	24: "RET_TSBLOCK_DATA_INCONSISTENCY",
	26: "RET_TYPE_NOT_SUPPORTED",
	27: "RET_TYPE_NOT_MATCH",
	28: "RET_FILE_OPEN_ERR",
	29: "RET_FILE_CLOSE_ERR",
	30: "RET_FILE_WRITE_ERR",
	31: "RET_FILE_READ_ERR",
	32: "RET_FILE_SYNC_ERR",
	33: "RET_TSFILE_WRITER_META_ERR",
	34: "RET_FILE_STAT_ERR",
	35: "RET_TSFILE_CORRUPTED",
	36: "RET_BUF_NOT_ENOUGH",
	37: "RET_INVALID_PATH",
	38: "RET_NOT_MATCH",
	40: "RET_NOT_SUPPORT",
	43: "RET_INVALID_DATA_POINT",
	44: "RET_DEVICE_NOT_EXIST",
	45: "RET_MEASUREMENT_NOT_EXIST",
	48: "RET_COMPRESS_ERR",
	49: "RET_TABLE_NOT_EXIST",
	50: "RET_COLUMN_NOT_EXIST",
	51: "RET_UNSUPPORTED_ORDER",
	52: "RET_INVALID_NODE_TYPE",
	53: "RET_ENCODE_ERR",
	54: "RET_DECODE_ERR",
}

func assertPinned(t *testing.T, kind string, cases map[string]int32, actual func(string) int32) {
	t.Helper()
	for name, expected := range cases {
		if got := actual(name); got != expected {
			t.Errorf("%s.%s = %d, want %d", kind, name, got, expected)
		}
	}
}

func TestDataTypeValuesMatchCHeader(t *testing.T) {
	assertPinned(t, "DataType", map[string]int32{
		"DataTypeBoolean":   0,   // TS_DATATYPE_BOOLEAN
		"DataTypeInt32":     1,   // TS_DATATYPE_INT32
		"DataTypeInt64":     2,   // TS_DATATYPE_INT64
		"DataTypeFloat":     3,   // TS_DATATYPE_FLOAT
		"DataTypeDouble":    4,   // TS_DATATYPE_DOUBLE
		"DataTypeText":      5,   // TS_DATATYPE_TEXT
		"DataTypeVector":    6,   // TS_DATATYPE_VECTOR
		"DataTypeTimestamp": 8,   // TS_DATATYPE_TIMESTAMP
		"DataTypeDate":      9,   // TS_DATATYPE_DATE
		"DataTypeBlob":      10,  // TS_DATATYPE_BLOB
		"DataTypeString":    11,  // TS_DATATYPE_STRING
		"DataTypeNull":      254, // TS_DATATYPE_NULL_TYPE
		"DataTypeInvalid":   255, // TS_DATATYPE_INVALID
	}, func(name string) int32 {
		return int32(map[string]DataType{
			"DataTypeBoolean":   DataTypeBoolean,
			"DataTypeInt32":     DataTypeInt32,
			"DataTypeInt64":     DataTypeInt64,
			"DataTypeFloat":     DataTypeFloat,
			"DataTypeDouble":    DataTypeDouble,
			"DataTypeText":      DataTypeText,
			"DataTypeVector":    DataTypeVector,
			"DataTypeTimestamp": DataTypeTimestamp,
			"DataTypeDate":      DataTypeDate,
			"DataTypeBlob":      DataTypeBlob,
			"DataTypeString":    DataTypeString,
			"DataTypeNull":      DataTypeNull,
			"DataTypeInvalid":   DataTypeInvalid,
		}[name])
	})
}

func TestEncodingValuesMatchCHeader(t *testing.T) {
	assertPinned(t, "Encoding", map[string]int32{
		"EncodingPlain":      0,   // TS_ENCODING_PLAIN
		"EncodingDictionary": 1,   // TS_ENCODING_DICTIONARY
		"EncodingRLE":        2,   // TS_ENCODING_RLE
		"EncodingDiff":       3,   // TS_ENCODING_DIFF
		"EncodingTS2Diff":    4,   // TS_ENCODING_TS_2DIFF
		"EncodingBitmap":     5,   // TS_ENCODING_BITMAP
		"EncodingGorillaV1":  6,   // TS_ENCODING_GORILLA_V1
		"EncodingRegular":    7,   // TS_ENCODING_REGULAR
		"EncodingGorilla":    8,   // TS_ENCODING_GORILLA
		"EncodingZigZag":     9,   // TS_ENCODING_ZIGZAG
		"EncodingFreq":       10,  // TS_ENCODING_FREQ
		"EncodingChimp":      11,  // TS_ENCODING_CHIMP
		"EncodingSprintz":    12,  // TS_ENCODING_SPRINTZ
		"EncodingRLBE":       13,  // TS_ENCODING_RLBE
		"EncodingCamel":      14,  // TS_ENCODING_CAMEL
		"EncodingInvalid":    255, // TS_ENCODING_INVALID
	}, func(name string) int32 {
		return int32(map[string]Encoding{
			"EncodingPlain":      EncodingPlain,
			"EncodingDictionary": EncodingDictionary,
			"EncodingRLE":        EncodingRLE,
			"EncodingDiff":       EncodingDiff,
			"EncodingTS2Diff":    EncodingTS2Diff,
			"EncodingBitmap":     EncodingBitmap,
			"EncodingGorillaV1":  EncodingGorillaV1,
			"EncodingRegular":    EncodingRegular,
			"EncodingGorilla":    EncodingGorilla,
			"EncodingZigZag":     EncodingZigZag,
			"EncodingFreq":       EncodingFreq,
			"EncodingChimp":      EncodingChimp,
			"EncodingSprintz":    EncodingSprintz,
			"EncodingRLBE":       EncodingRLBE,
			"EncodingCamel":      EncodingCamel,
			"EncodingInvalid":    EncodingInvalid,
		}[name])
	})
}

func TestCompressionValuesMatchCHeader(t *testing.T) {
	assertPinned(t, "Compression", map[string]int32{
		"CompressionUncompressed": 0,   // TS_COMPRESSION_UNCOMPRESSED
		"CompressionSnappy":       1,   // TS_COMPRESSION_SNAPPY
		"CompressionGzip":         2,   // TS_COMPRESSION_GZIP
		"CompressionLZO":          3,   // TS_COMPRESSION_LZO
		"CompressionSDT":          4,   // TS_COMPRESSION_SDT
		"CompressionPAA":          5,   // TS_COMPRESSION_PAA
		"CompressionPLA":          6,   // TS_COMPRESSION_PLA
		"CompressionLZ4":          7,   // TS_COMPRESSION_LZ4
		"CompressionZstd":         8,   // TS_COMPRESSION_ZSTD
		"CompressionLZMA2":        9,   // TS_COMPRESSION_LZMA2
		"CompressionInvalid":      255, // TS_COMPRESSION_INVALID
	}, func(name string) int32 {
		return int32(map[string]Compression{
			"CompressionUncompressed": CompressionUncompressed,
			"CompressionSnappy":       CompressionSnappy,
			"CompressionGzip":         CompressionGzip,
			"CompressionLZO":          CompressionLZO,
			"CompressionSDT":          CompressionSDT,
			"CompressionPAA":          CompressionPAA,
			"CompressionPLA":          CompressionPLA,
			"CompressionLZ4":          CompressionLZ4,
			"CompressionZstd":         CompressionZstd,
			"CompressionLZMA2":        CompressionLZMA2,
			"CompressionInvalid":      CompressionInvalid,
		}[name])
	})
}

func TestColumnCategoryValuesMatchCHeader(t *testing.T) {
	assertPinned(t, "ColumnCategory", map[string]int32{
		"ColumnCategoryTag":       0, // TAG
		"ColumnCategoryField":     1, // FIELD
		"ColumnCategoryAttribute": 2, // ATTRIBUTE
		"ColumnCategoryTime":      3, // TIME
	}, func(name string) int32 {
		return int32(map[string]ColumnCategory{
			"ColumnCategoryTag":       ColumnCategoryTag,
			"ColumnCategoryField":     ColumnCategoryField,
			"ColumnCategoryAttribute": ColumnCategoryAttribute,
			"ColumnCategoryTime":      ColumnCategoryTime,
		}[name])
	})
}

func TestErrorMessagesCoverAllCErrorCodes(t *testing.T) {
	for code, name := range cErrnoCodes {
		message := errorCodeMessage(code)
		if message == "" {
			t.Errorf("%s (%d): message is empty", name, code)
		} else if message == fmt.Sprintf("unknown error code %d", code) {
			t.Errorf("%s (%d) is mapped to the unknown-code message", name, code)
		}
	}
}

func TestErrorMessagesAreUnique(t *testing.T) {
	seen := make(map[string]int32, len(cErrnoCodes))
	for code, name := range cErrnoCodes {
		message := errorCodeMessage(code)
		if other, ok := seen[message]; ok {
			t.Errorf("codes %d (%s) and %d (%s) share message %q", other, cErrnoCodes[other], code, name, message)
		}
		seen[message] = code
	}
}

func TestSentinelErrorsMatchCErrorCodes(t *testing.T) {
	want := map[*Error]int32{
		ErrInvalidArgument:     4,  // RET_INVALID_ARG
		ErrOutOfRange:          5,  // RET_OUT_OF_RANGE
		ErrInvalidSchema:       8,  // RET_INVALID_SCHEMA
		ErrTypeNotSupported:    26, // RET_TYPE_NOT_SUPPORTED
		ErrTypeMismatch:        27, // RET_TYPE_NOT_MATCH
		ErrFileOpen:            28, // RET_FILE_OPEN_ERR
		ErrFileClose:           29, // RET_FILE_CLOSE_ERR
		ErrFileWrite:           30, // RET_FILE_WRITE_ERR
		ErrFileRead:            31, // RET_FILE_READ_ERR
		ErrInvalidPath:         37, // RET_INVALID_PATH
		ErrDeviceNotExist:      44, // RET_DEVICE_NOT_EXIST
		ErrMeasurementNotExist: 45, // RET_MEASUREMENT_NOT_EXIST
		ErrTableNotExist:       49, // RET_TABLE_NOT_EXIST
		ErrColumnNotExist:      50, // RET_COLUMN_NOT_EXIST
	}
	for sentinel, code := range want {
		if sentinel.Code != code {
			t.Errorf("sentinel %#v has code %d, want %d", sentinel, sentinel.Code, code)
		}
		if _, known := cErrnoCodes[code]; !known {
			t.Errorf("sentinel %#v maps to code %d not defined in errno_define_c.h", sentinel, code)
		}
	}
}
