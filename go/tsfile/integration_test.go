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
	"path/filepath"
	"sync"
	"testing"
)

func TestTreeRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tree.tsfile")
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	schema := DeviceSchema{Device: "root.d1", TimeSeries: []TimeseriesSchema{
		{Name: "s1", DataType: DataTypeInt64, Encoding: EncodingPlain, Compression: CompressionUncompressed},
		{Name: "label", DataType: DataTypeString, Encoding: EncodingPlain, Compression: CompressionUncompressed},
	}}
	if err := writer.RegisterDevice(schema); err != nil {
		t.Fatal(err)
	}
	tablet, err := NewTablet("root.d1", []TabletColumn{
		{Name: "s1", DataType: DataTypeInt64},
		{Name: "label", DataType: DataTypeString},
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()
	for row, timestamp := range []int64{10, 20} {
		if err := tablet.AddTimestamp(row, timestamp); err != nil {
			t.Fatal(err)
		}
		if err := tablet.SetInt64(row, 0, timestamp*10); err != nil {
			t.Fatal(err)
		}
		if err := tablet.SetString(row, 1, []string{"first", "second"}[row]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.WriteTreeTablet(tablet); err != nil {
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
	result, err := reader.QueryTree(TreeQuery{Paths: []string{"root.d1.s1", "root.d1.label"}, Start: 0, End: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	metadata := result.Metadata()
	if len(metadata) != 3 {
		t.Fatalf("metadata columns = %v, want timestamp and two values", metadata)
	}
	for row, wantTimestamp := range []int64{10, 20} {
		ok, err := result.Next()
		if err != nil || !ok {
			t.Fatalf("row %d Next = %v, %v", row, ok, err)
		}
		if got, err := result.Int64(0); err != nil || got != wantTimestamp {
			t.Fatalf("timestamp = %d, %v; want %d", got, err, wantTimestamp)
		}
		if got, err := result.Int64(1); err != nil || got != wantTimestamp*10 {
			t.Fatalf("s1 = %d, %v", got, err)
		}
		if got, err := result.String(2); err != nil || got != []string{"first", "second"}[row] {
			t.Fatalf("label = %q, %v", got, err)
		}
	}
	if ok, err := result.Next(); err != nil || ok {
		t.Fatalf("end Next = %v, %v; want false, nil", ok, err)
	}
	if err := result.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := reader.QueryTreeRows(TreeRowsQuery{
		Devices: []string{"root.d1"}, Measurements: []string{"s1"}, Offset: 1, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if ok, err := rows.Next(); err != nil || !ok {
		t.Fatalf("row query Next = %v, %v", ok, err)
	}
	if got, err := rows.Int64(0); err != nil || got != 20 {
		t.Fatalf("row query timestamp = %d, %v", got, err)
	}
	if got, err := rows.Int64(1); err != nil || got != 200 {
		t.Fatalf("row query s1 = %d, %v", got, err)
	}
}

func TestTableRoundTripAndNull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.tsfile")
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.RegisterTable(TableSchema{Table: "metrics", Columns: []ColumnSchema{
		{Name: "device", DataType: DataTypeString, Category: ColumnCategoryTag},
		{Name: "value", DataType: DataTypeDouble, Category: ColumnCategoryField},
		{Name: "enabled", DataType: DataTypeBoolean, Category: ColumnCategoryField},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddProperty("source", []byte("go-integration")); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddProperty("empty", []byte{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty property value error = %v, want ErrInvalidArgument", err)
	}
	tablet, err := NewTablet("metrics", []TabletColumn{
		{Name: "device", DataType: DataTypeString},
		{Name: "value", DataType: DataTypeDouble},
		{Name: "enabled", DataType: DataTypeBoolean},
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
	result, err := reader.QueryTable(TableQuery{Table: "metrics", Columns: []string{"device", "value", "enabled"}, Start: 0, End: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	for row := 0; row < 3; row++ {
		ok, err := result.Next()
		if err != nil || !ok {
			t.Fatalf("row %d Next = %v, %v", row, ok, err)
		}
		if got, err := result.String(1); err != nil || got != "d1" {
			t.Fatalf("device = %q, %v", got, err)
		}
		isNull, err := result.IsNull(2)
		if err != nil || isNull != (row == 2) {
			t.Fatalf("row %d null = %v, %v", row, isNull, err)
		}
		if row == 2 {
			if _, err := result.Float64(2); !errors.Is(err, ErrNullValue) {
				t.Fatalf("null Float64 error = %v", err)
			}
		} else if got, err := result.Float64(2); err != nil || got != float64(row)+0.5 {
			t.Fatalf("value = %v, %v", got, err)
		}
		if _, err := result.Int64(1); !errors.Is(err, ErrTypeMismatch) {
			t.Fatalf("type mismatch error = %v", err)
		}
	}
	if err := result.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := reader.QueryTableRows(TableRowsQuery{
		Table: "metrics", Columns: []string{"device", "value"}, Offset: 1, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if ok, err := rows.Next(); err != nil || !ok {
		t.Fatalf("table row query Next = %v, %v", ok, err)
	}
	if got, err := rows.Int64(0); err != nil || got != 2 {
		t.Fatalf("table row timestamp = %d, %v", got, err)
	}
	if got, err := rows.Float64(2); err != nil || got != 1.5 {
		t.Fatalf("table row value = %v, %v", got, err)
	}
}

func TestReaderCloseClosesResultsConcurrently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifetime.tsfile")
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.RegisterTimeseries("root.d1", TimeseriesSchema{Name: "s1", DataType: DataTypeInt64, Encoding: EncodingPlain, Compression: CompressionUncompressed}); err != nil {
		t.Fatal(err)
	}
	tablet, err := NewTablet("root.d1", []TabletColumn{{Name: "s1", DataType: DataTypeInt64}}, 1)
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
	if err := writer.WriteTreeTablet(tablet); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.QueryTree(TreeQuery{Paths: []string{"root.d1.s1"}, Start: 0, End: 2})
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
