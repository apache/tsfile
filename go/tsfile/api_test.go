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
	if _, err := NewTablet("root.d1", nil, 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty columns: %v", err)
	}
	tablet, err := NewTablet("root.d1", []TabletColumn{
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

func TestWriterOptionValidation(t *testing.T) {
	if _, err := NewWriter("unused.tsfile", WithMemoryThreshold(0)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero memory threshold: %v", err)
	}
	if _, err := NewWriter("unused.tsfile", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil option: %v", err)
	}
}
