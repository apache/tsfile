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
	"testing"
)

func TestTabletValidationAndClose(t *testing.T) {
	if _, err := NewTablet(nil, 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty columns: %v", err)
	}
	if _, err := NewTablet([]TabletColumn{{
		Name: "value", DataType: DataType(255),
	}}, 1); !errors.Is(err, ErrTypeNotSupported) {
		t.Fatalf("unsupported data type: %v", err)
	}
	tablet, err := NewTablet([]TabletColumn{
		{Name: "text", DataType: DataTypeString},
		{Name: "number", DataType: DataTypeInt64},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tablet.SetInt64(0, 0, 1); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("type mismatch: %v", err)
	}
	if err := tablet.SetString(0, 0, "a\x00b"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("embedded NUL: %v", err)
	}
	if err := tablet.AddTimestamp(1, 1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("row out of range: %v", err)
	}
	if err := tablet.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tablet.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := tablet.AddTimestamp(0, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
}

func TestTabletNormalizesAndOwnsColumnNames(t *testing.T) {
	columns := []TabletColumn{{Name: "Value", DataType: DataTypeInt64}}
	tablet, err := NewTablet(columns, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()
	if columns[0].Name != "Value" || tablet.columns[0].Name != "value" {
		t.Fatalf("caller columns = %+v, tablet columns = %+v", columns, tablet.columns)
	}
	if _, err := NewTablet([]TabletColumn{
		{Name: "value", DataType: DataTypeInt64},
		{Name: "VALUE", DataType: DataTypeInt64},
	}, 1); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("case-insensitive duplicate error = %v", err)
	}
}

func TestTabletRowsFollowNativeTimestampRange(t *testing.T) {
	tablet, err := NewTablet([]TabletColumn{{Name: "value", DataType: DataTypeInt64}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()

	if err := tablet.SetInt64(0, 0, 42); err != nil {
		t.Fatal(err)
	}
	if got := tablet.Rows(); got != 0 {
		t.Fatalf("Rows after setting only a value = %d, want native row count 0", got)
	}
	if err := tablet.AddTimestamp(1, 100); err != nil {
		t.Fatal(err)
	}
	if got := tablet.Rows(); got != 2 {
		t.Fatalf("Rows after setting timestamp row 1 = %d, want native row count 2", got)
	}
}

func TestWriterOptionValidation(t *testing.T) {
	if _, err := NewWriter("unused.tsfile", validTestSchema(), WithMemoryThreshold(0)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero memory threshold: %v", err)
	}
	if _, err := NewWriter("unused.tsfile", validTestSchema(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil option: %v", err)
	}
}
