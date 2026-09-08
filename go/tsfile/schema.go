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

import "fmt"

// TimeseriesSchema describes one tree-model measurement.
type TimeseriesSchema struct {
	Name        string
	DataType    DataType
	Encoding    Encoding
	Compression Compression
}

// DeviceSchema groups tree-model measurements under one device.
type DeviceSchema struct {
	Device     string
	TimeSeries []TimeseriesSchema
}

// ColumnSchema describes one table-model column.
type ColumnSchema struct {
	Name     string
	DataType DataType
	Category ColumnCategory
}

// TableSchema describes a table-model table.
type TableSchema struct {
	Table   string
	Columns []ColumnSchema
}

// TabletColumn fixes the name and type of a Tablet column.
type TabletColumn struct {
	Name     string
	DataType DataType
}

// ColumnMetadata describes one column in a query result. Column zero is the
// timestamp column returned by the native result set.
type ColumnMetadata struct {
	Name     string
	DataType DataType
}

func timeseriesNames(series []TimeseriesSchema) []string {
	names := make([]string, len(series))
	for i := range series {
		names[i] = series[i].Name
	}
	return names
}

func columnNames(columns []ColumnSchema) []string {
	names := make([]string, len(columns))
	for i := range columns {
		names[i] = columns[i].Name
	}
	return names
}

func validDataType(value DataType) bool {
	switch value {
	case DataTypeBoolean, DataTypeInt32, DataTypeInt64, DataTypeFloat,
		DataTypeDouble, DataTypeText, DataTypeTimestamp, DataTypeDate,
		DataTypeString:
		return true
	default:
		return false
	}
}

func validEncoding(value Encoding) bool {
	return value >= EncodingPlain && value <= EncodingCamel
}

func validCompression(value Compression) bool {
	return value >= CompressionUncompressed && value <= CompressionLZMA2
}

func validColumnCategory(value ColumnCategory) bool {
	return value >= ColumnCategoryTag && value <= ColumnCategoryTime
}

func validateTimeseriesSchema(op string, schema TimeseriesSchema) error {
	if err := validateCString(op, "timeseries name", schema.Name); err != nil {
		return err
	}
	if !validDataType(schema.DataType) || !validEncoding(schema.Encoding) ||
		!validCompression(schema.Compression) {
		return fmt.Errorf("%w: invalid timeseries schema for %q", newError(op, 8), schema.Name)
	}
	return nil
}
