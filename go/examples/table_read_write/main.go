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

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/tsfile/go/tsfile"
)

const (
	tabletRowCount = 10
	arrowRowCount  = 5
)

var schema = tsfile.TableSchema{Table: "metrics", Columns: []tsfile.ColumnSchema{
	{Name: "device", DataType: tsfile.DataTypeString, Category: tsfile.ColumnCategoryTag},
	{Name: "value", DataType: tsfile.DataTypeInt64, Category: tsfile.ColumnCategoryField},
}}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	path := "table-example.tsfile"
	_ = os.Remove(path)

	writer, err := tsfile.NewWriter(path, schema)
	if err != nil {
		return err
	}
	if err := writeTabletRows(writer, 1, tabletRowCount); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writeArrowRows(writer, tabletRowCount+1, arrowRowCount); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	return readRows(path)
}

func writeTabletRows(writer *tsfile.Writer, firstTimestamp, rowCount int) error {
	// tsfile.Tablet is the Go-facing wrapper around a native C++
	// storage::Tablet. These setters validate indexes and types in Go, then
	// populate the native batch through cgo.
	tablet, err := tsfile.NewTablet([]tsfile.TabletColumn{
		{Name: "device", DataType: tsfile.DataTypeString},
		{Name: "value", DataType: tsfile.DataTypeInt64},
	}, rowCount)
	if err != nil {
		return err
	}
	defer tablet.Close()

	for row := 0; row < rowCount; row++ {
		timestamp := int64(firstTimestamp + row)
		if err := tablet.AddTimestamp(row, timestamp); err != nil {
			return err
		}
		if err := tablet.SetString(row, 0, "d1"); err != nil {
			return err
		}
		if err := tablet.SetInt64(row, 1, timestamp*10); err != nil {
			return err
		}
	}
	return writer.WriteTableTablet(tablet)
}

func writeArrowRows(writer *tsfile.Writer, firstTimestamp, rowCount int) error {
	// Arrow arrays are built by Arrow Go. WriteArrowBatch exports the record
	// through the Arrow C Data Interface, then C++ converts it to its native
	// Tablet representation before writing.
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "device", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	timeBuilder := builder.Field(0).(*array.Int64Builder)
	deviceBuilder := builder.Field(1).(*array.StringBuilder)
	valueBuilder := builder.Field(2).(*array.Int64Builder)
	for row := 0; row < rowCount; row++ {
		timestamp := int64(firstTimestamp + row)
		timeBuilder.Append(timestamp)
		deviceBuilder.Append("d1")
		valueBuilder.Append(timestamp * 10)
	}

	record := builder.NewRecord()
	defer record.Release()
	return writer.WriteArrowBatch(record)
}

func readRows(path string) error {
	reader, err := tsfile.NewReader(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	result, err := reader.Query("metrics", []string{"device", "value"})
	if err != nil {
		return err
	}
	defer result.Close()
	for {
		ok, err := result.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		timestamp, err := result.Int64(1)
		if err != nil {
			return err
		}
		device, err := result.String(2)
		if err != nil {
			return err
		}
		value, err := result.Int64(3)
		if err != nil {
			return err
		}
		fmt.Printf("%d %s %d\n", timestamp, device, value)
	}
}
