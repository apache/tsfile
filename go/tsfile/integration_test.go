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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestTableRoundTripAndNull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.tsfile")
	schema := TableSchema{Table: "metrics", Columns: []ColumnSchema{
		{Name: "device", DataType: DataTypeString, Category: ColumnCategoryTag},
		{Name: "value", DataType: DataTypeDouble, Category: ColumnCategoryField},
		{Name: "enabled", DataType: DataTypeBoolean, Category: ColumnCategoryField},
		{Name: "payload", DataType: DataTypeBlob, Category: ColumnCategoryField},
	}}
	writer, err := NewWriter(path, schema)
	if err != nil {
		t.Fatal(err)
	}
	schema.Table = "changed"
	schema.Columns[0].Name = "changed"
	if err := writer.AddProperty("source", []byte("go-integration")); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddProperty("empty", []byte{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty property value error = %v, want ErrInvalidArgument", err)
	}
	tablet, err := NewTablet([]TabletColumn{
		{Name: "device", DataType: DataTypeString},
		{Name: "value", DataType: DataTypeDouble},
		{Name: "enabled", DataType: DataTypeBoolean},
		{Name: "payload", DataType: DataTypeBlob},
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()
	for row := 0; row < 3; row++ {
		if err := tablet.AddTimestamp(row, int64(row+1)); err != nil {
			t.Fatal(err)
		}
		if err := tablet.SetString(row, 0, "d1"); err != nil {
			t.Fatal(err)
		}
		if row < 2 {
			if err := tablet.SetFloat64(row, 1, float64(row)+0.5); err != nil {
				t.Fatal(err)
			}
		}
		if err := tablet.SetBool(row, 2, row%2 == 0); err != nil {
			t.Fatal(err)
		}
		if row == 0 {
			if err := tablet.SetBytes(row, 3, []byte{'a', 0, 'b'}); err != nil {
				t.Fatal(err)
			}
		} else if row == 1 {
			if err := tablet.SetBytes(row, 3, []byte{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.WriteTableTablet(tablet); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	gotSchema, err := reader.GetTableSchema("metrics")
	if err != nil || gotSchema.Table != "metrics" || len(gotSchema.Columns) != 4 {
		t.Fatalf("GetTableSchema = %+v, %v", gotSchema, err)
	}
	schemas, err := reader.GetAllTableSchemas()
	if err != nil || len(schemas) != 1 || schemas[0].Table != "metrics" {
		t.Fatalf("GetAllTableSchemas = %+v, %v", schemas, err)
	}
	if _, err := reader.GetTableSchema("missing"); !errors.Is(err, ErrTableNotExist) {
		t.Fatalf("missing schema error = %v", err)
	}
	result, err := reader.Query("metrics", []string{"device", "value", "enabled", "payload"}, WithLimit(-2))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	for row := 0; row < 3; row++ {
		ok, err := result.Next()
		if err != nil || !ok {
			t.Fatalf("row %d Next = %v, %v", row, ok, err)
		}
		if got, err := result.Int64(1); err != nil || got != int64(row+1) {
			t.Fatalf("timestamp = %d, %v", got, err)
		}
		if got, err := result.String(2); err != nil || got != "d1" {
			t.Fatalf("device = %q, %v", got, err)
		}
		isNull, err := result.IsNull(3)
		if err != nil || isNull != (row == 2) {
			t.Fatalf("row %d null = %v, %v", row, isNull, err)
		}
		if row == 2 {
			if _, err := result.Float64(3); !errors.Is(err, ErrNullValue) {
				t.Fatalf("null Float64 error = %v", err)
			}
		} else if got, err := result.Float64(3); err != nil || got != float64(row)+0.5 {
			t.Fatalf("value = %v, %v", got, err)
		}
		payloadNull, err := result.IsNull(5)
		if err != nil || payloadNull != (row == 2) {
			t.Fatalf("row %d payload null = %v, %v", row, payloadNull, err)
		}
		if row < 2 {
			payload, err := result.Bytes(5)
			if err != nil {
				t.Fatal(err)
			}
			want := [][]byte{{'a', 0, 'b'}, {}}[row]
			if string(payload) != string(want) {
				t.Fatalf("row %d payload = %v, want %v", row, payload, want)
			}
		}
	}

	page, err := reader.Query("metrics", []string{"value"},
		WithTimeRange(1, 2),
		WithTagFilter(Eq("device", "d1")),
		WithOffset(1),
		WithLimit(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer page.Close()
	if ok, err := page.Next(); err != nil || !ok {
		t.Fatalf("page Next = %v, %v", ok, err)
	}
	if got, err := page.Int64(1); err != nil || got != 2 {
		t.Fatalf("page timestamp = %d, %v", got, err)
	}
	if got, err := page.Float64(2); err != nil || got != 1.5 {
		t.Fatalf("page value = %v, %v", got, err)
	}
}

func TestNewWriterTruncatesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.tsfile")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path, TableSchema{Table: "metrics", Columns: []ColumnSchema{
		{Name: "value", DataType: DataTypeInt64, Category: ColumnCategoryField},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTableIdentifiersAreCaseInsensitive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "case-insensitive.tsfile")
	writer, err := NewWriter(path, TableSchema{Table: "Metrics", Columns: []ColumnSchema{
		{Name: "Device", DataType: DataTypeString, Category: ColumnCategoryTag},
		{Name: "Value", DataType: DataTypeInt64, Category: ColumnCategoryField},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tablet, err := NewTablet([]TabletColumn{
		{Name: "DEVICE", DataType: DataTypeString},
		{Name: "value", DataType: DataTypeInt64},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()
	if err := tablet.AddTimestamp(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetString(0, 0, "d1"); err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetInt64(0, 1, 42); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteTableTablet(tablet); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	result, err := reader.Query("METRICS", []string{"VALUE"}, WithTagFilter(Eq("DEVICE", "d1")))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if ok, err := result.Next(); err != nil || !ok {
		t.Fatalf("Next = %v, %v", ok, err)
	}
	if got, err := result.Int64(2); err != nil || got != 42 {
		t.Fatalf("value = %d, %v", got, err)
	}
}

func TestReaderCloseClosesTableResultsConcurrently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifetime.tsfile")
	writer, err := NewWriter(path, TableSchema{Table: "metrics", Columns: []ColumnSchema{
		{Name: "value", DataType: DataTypeInt64, Category: ColumnCategoryField},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tablet, err := NewTablet([]TabletColumn{{Name: "value", DataType: DataTypeInt64}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()
	if err := tablet.AddTimestamp(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetInt64(0, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteTableTablet(tablet); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Query("metrics", []string{"value"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = result.Metadata()
		}
	}()
	go func() { defer wg.Done(); _ = reader.Close() }()
	wg.Wait()
	if _, err := result.Next(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next after Reader.Close = %v", err)
	}
	if err := result.Close(); err != nil {
		t.Fatalf("ResultSet.Close after Reader.Close = %v", err)
	}
}

func TestTabletWriteValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tablet-validation.tsfile")
	writer, err := NewWriter(path, TableSchema{Table: "metrics", Columns: []ColumnSchema{
		{Name: "value", DataType: DataTypeInt64, Category: ColumnCategoryField},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	wrong, err := NewTablet([]TabletColumn{{Name: "other", DataType: DataTypeInt64}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()
	if err := wrong.AddTimestamp(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteTableTablet(wrong); !errors.Is(err, ErrColumnNotExist) {
		t.Fatalf("wrong Tablet schema error = %v", err)
	}

	missingTime, err := NewTablet([]TabletColumn{{Name: "value", DataType: DataTypeInt64}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer missingTime.Close()
	if err := missingTime.SetInt64(0, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteTableTablet(missingTime); err != nil {
		t.Fatalf("native-accepted Tablet rejected by Go validation: %v", err)
	}
}

func TestTabletCanBeReusedAcrossWriters(t *testing.T) {
	tablet, err := NewTablet([]TabletColumn{
		{Name: "device", DataType: DataTypeString},
		{Name: "value", DataType: DataTypeInt64},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()
	if err := tablet.AddTimestamp(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetString(0, 0, "d1"); err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetInt64(0, 1, 42); err != nil {
		t.Fatal(err)
	}

	newSchema := func(table string) TableSchema {
		return TableSchema{Table: table, Columns: []ColumnSchema{
			{Name: "device", DataType: DataTypeString, Category: ColumnCategoryTag},
			{Name: "value", DataType: DataTypeInt64, Category: ColumnCategoryField},
		}}
	}
	for _, table := range []string{"first", "second"} {
		path := filepath.Join(t.TempDir(), table+".tsfile")
		writer, err := NewWriter(path, newSchema(table))
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteTableTablet(tablet); err != nil {
			_ = writer.Close()
			t.Fatalf("write shared Tablet to %q: %v", table, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		reader, err := NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		result, err := reader.Query(table, []string{"value"})
		if err != nil {
			_ = reader.Close()
			t.Fatal(err)
		}
		ok, nextErr := result.Next()
		value, valueErr := result.Int64(2)
		if closeErr := result.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if nextErr != nil || !ok || valueErr != nil || value != 42 {
			t.Fatalf("read %q: Next = %v, %v; value = %d, %v", table, ok, nextErr, value, valueErr)
		}
	}
}

func TestTabletRoundTripAllTableTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "all-types.tsfile")
	columns := []ColumnSchema{
		{Name: "tag", DataType: DataTypeString, Category: ColumnCategoryTag},
		{Name: "boolean", DataType: DataTypeBoolean, Category: ColumnCategoryField},
		{Name: "int32", DataType: DataTypeInt32, Category: ColumnCategoryField},
		{Name: "int64", DataType: DataTypeInt64, Category: ColumnCategoryField},
		{Name: "float", DataType: DataTypeFloat, Category: ColumnCategoryField},
		{Name: "double", DataType: DataTypeDouble, Category: ColumnCategoryField},
		{Name: "text", DataType: DataTypeText, Category: ColumnCategoryField},
		{Name: "timestamp", DataType: DataTypeTimestamp, Category: ColumnCategoryField},
		{Name: "date", DataType: DataTypeDate, Category: ColumnCategoryField},
		{Name: "blob", DataType: DataTypeBlob, Category: ColumnCategoryField},
	}
	writer, err := NewWriter(path, TableSchema{Table: "types", Columns: columns})
	if err != nil {
		t.Fatal(err)
	}
	tabletColumns := make([]TabletColumn, len(columns))
	for i, column := range columns {
		tabletColumns[i] = TabletColumn{Name: column.Name, DataType: column.DataType}
	}
	tablet, err := NewTablet(tabletColumns, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()
	checks := []error{
		tablet.AddTimestamp(0, 100),
		tablet.SetString(0, 0, "d1"),
		tablet.SetBool(0, 1, true),
		tablet.SetInt32(0, 2, -12),
		tablet.SetInt64(0, 3, 34),
		tablet.SetFloat32(0, 4, 1.25),
		tablet.SetFloat64(0, 5, 2.5),
		tablet.SetString(0, 6, "text"),
		tablet.SetInt64(0, 7, 123456),
		tablet.SetInt32(0, 8, 20260909),
		tablet.SetBytes(0, 9, []byte{0, 1, 2}),
	}
	for _, err := range checks {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.WriteTableTablet(tablet); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	result, err := reader.Query("types", names)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if ok, err := result.Next(); err != nil || !ok {
		t.Fatalf("Next = %v, %v", ok, err)
	}
	if value, err := result.Int64(1); err != nil || value != 100 {
		t.Fatalf("time = %d, %v", value, err)
	}
	if value, err := result.String(2); err != nil || value != "d1" {
		t.Fatalf("tag = %q, %v", value, err)
	}
	if value, err := result.Bool(3); err != nil || !value {
		t.Fatalf("boolean = %v, %v", value, err)
	}
	if value, err := result.Int32(4); err != nil || value != -12 {
		t.Fatalf("int32 = %d, %v", value, err)
	}
	if value, err := result.Int64(5); err != nil || value != 34 {
		t.Fatalf("int64 = %d, %v", value, err)
	}
	if value, err := result.Float32(6); err != nil || value != 1.25 {
		t.Fatalf("float = %f, %v", value, err)
	}
	if value, err := result.Float64(7); err != nil || value != 2.5 {
		t.Fatalf("double = %f, %v", value, err)
	}
	if value, err := result.String(8); err != nil || value != "text" {
		t.Fatalf("text = %q, %v", value, err)
	}
	if value, err := result.Int64(9); err != nil || value != 123456 {
		t.Fatalf("timestamp = %d, %v", value, err)
	}
	if value, err := result.Int32(10); err != nil || value != 20260909 {
		t.Fatalf("date = %d, %v", value, err)
	}
	if value, err := result.Bytes(11); err != nil || string(value) != string([]byte{0, 1, 2}) {
		t.Fatalf("blob = %v, %v", value, err)
	}
}

func TestArrowBatchRoundTripAndModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arrow.tsfile")
	schema := TableSchema{Table: "metrics", Columns: []ColumnSchema{
		{Name: "device", DataType: DataTypeString, Category: ColumnCategoryTag},
		{Name: "value", DataType: DataTypeDouble, Category: ColumnCategoryField},
		{Name: "payload", DataType: DataTypeBlob, Category: ColumnCategoryField},
	}}
	writer, err := NewWriter(path, schema)
	if err != nil {
		t.Fatal(err)
	}
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "device", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"d1", "d1", "d1"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1, 0, 3}, []bool{true, false, true})
	builder.Field(3).(*array.BinaryBuilder).AppendValues([][]byte{{'a', 0, 'b'}, {}, nil}, []bool{true, true, false})
	record := builder.NewRecord()
	if err := writer.WriteArrowBatch(record); err != nil {
		record.Release()
		t.Fatal(err)
	}
	record.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{40, 50}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"d1", "d1"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{4, 5}, nil)
	builder.Field(3).(*array.BinaryBuilder).AppendValues([][]byte{{'x'}, {'y'}}, nil)
	secondRecord := builder.NewRecord()
	tableInput := array.NewTableFromRecords(arrowSchema, []arrow.Record{secondRecord})
	secondRecord.Release()
	if err := writer.WriteArrowBatch(tableInput); err != nil {
		tableInput.Release()
		t.Fatal(err)
	}
	tableInput.Release()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	result, err := reader.Query("metrics", []string{"device", "value", "payload"}, WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if _, err := result.Next(); !errors.Is(err, ErrWrongResultMode) {
		t.Fatalf("Next in batch mode = %v", err)
	}
	var rows int64
	var times []int64
	var values []float64
	var valueValid []bool
	var payloads [][]byte
	var payloadValid []bool
	for {
		batch, err := result.ReadArrowRecordBatch()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows += batch.NumRows()
		if batch.Schema().Field(0).Name != "time" || batch.Schema().Field(3).Name != "payload" {
			t.Fatalf("unexpected Arrow schema: %s", batch.Schema())
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			switch column := batch.Column(0).(type) {
			case *array.Int64:
				times = append(times, column.Value(i))
			case *array.Timestamp:
				times = append(times, int64(column.Value(i)))
			default:
				t.Fatalf("unexpected time array %T", column)
			}
			valueColumn := batch.Column(2).(*array.Float64)
			valueValid = append(valueValid, !valueColumn.IsNull(i))
			values = append(values, valueColumn.Value(i))
			payloadColumn := batch.Column(3).(*array.Binary)
			payloadValid = append(payloadValid, !payloadColumn.IsNull(i))
			payloads = append(payloads, append([]byte(nil), payloadColumn.Value(i)...))
		}
		batch.Release()
	}
	if rows != 5 {
		t.Fatalf("Arrow rows = %d, want 5", rows)
	}
	if fmt.Sprint(times) != "[10 20 30 40 50]" {
		t.Fatalf("Arrow times = %v", times)
	}
	if fmt.Sprint(valueValid) != "[true false true true true]" || values[0] != 1 || values[4] != 5 {
		t.Fatalf("Arrow values = %v, valid = %v", values, valueValid)
	}
	if fmt.Sprint(payloadValid) != "[true true false true true]" ||
		string(payloads[0]) != "a\x00b" || len(payloads[1]) != 0 {
		t.Fatalf("Arrow payloads = %v, valid = %v", payloads, payloadValid)
	}

	tableResult, err := reader.Query("metrics", []string{"value"}, WithBatchSize(8))
	if err != nil {
		t.Fatal(err)
	}
	table, err := tableResult.ReadArrowBatch()
	if err != nil {
		t.Fatal(err)
	}
	if table.NumRows() != 5 || table.NumCols() != 2 {
		t.Fatalf("Arrow table shape = %dx%d", table.NumRows(), table.NumCols())
	}
	table.Release()
	if _, err := tableResult.ReadArrowBatch(); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadArrowBatch EOF = %v", err)
	}
	if err := tableResult.Close(); err != nil {
		t.Fatal(err)
	}

	page, err := reader.Query("metrics", []string{"value"},
		WithTimeRange(10, 40), WithTagFilter(Eq("device", "d1")),
		WithOffset(1), WithLimit(2), WithBatchSize(1))
	if err != nil {
		t.Fatal(err)
	}
	owned, err := page.ReadArrowRecordBatch()
	if err != nil {
		t.Fatal(err)
	}
	if err := page.Close(); err != nil {
		owned.Release()
		t.Fatal(err)
	}
	if owned.NumRows() != 1 {
		owned.Release()
		t.Fatalf("owned batch rows after ResultSet.Close = %d", owned.NumRows())
	}
	owned.Release()

	empty, err := reader.Query("metrics", []string{"value"},
		WithTagFilter(Eq("device", "missing")), WithBatchSize(4))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if _, err := empty.ReadArrowRecordBatch(); !errors.Is(err, io.EOF) {
		t.Fatalf("empty Arrow query = %v", err)
	}
}

func TestArrowTimestampFieldRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arrow-timestamp.tsfile")
	schema := TableSchema{Table: "events", Columns: []ColumnSchema{
		{Name: "device", DataType: DataTypeString, Category: ColumnCategoryTag},
		{Name: "observed_at", DataType: DataTypeTimestamp, Category: ColumnCategoryField},
	}}
	writer, err := NewWriter(path, schema)
	if err != nil {
		t.Fatal(err)
	}
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "device", Type: arrow.BinaryTypes.String},
		{Name: "observed_at", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"d1", "d1"}, nil)
	builder.Field(2).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{123456789, 0}, []bool{true, false})
	record := builder.NewRecord()
	if err := writer.WriteArrowBatch(record); err != nil {
		record.Release()
		builder.Release()
		_ = writer.Close()
		t.Fatal(err)
	}
	record.Release()
	builder.Release()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	result, err := reader.Query("events", []string{"observed_at"})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if ok, err := result.Next(); err != nil || !ok {
		t.Fatalf("first Next = %v, %v", ok, err)
	}
	if value, err := result.Int64(2); err != nil || value != 123456789 {
		t.Fatalf("first timestamp = %d, %v", value, err)
	}
	if ok, err := result.Next(); err != nil || !ok {
		t.Fatalf("second Next = %v, %v", ok, err)
	}
	if isNull, err := result.IsNull(2); err != nil || !isNull {
		t.Fatalf("second timestamp null = %v, %v", isNull, err)
	}
}
