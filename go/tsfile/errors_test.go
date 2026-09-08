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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

package tsfile

import (
	"errors"
	"testing"
)

func TestNewErrorNilForOK(t *testing.T) {
	if got := newError("op", 0); got != nil {
		t.Fatalf("newError(op, 0) = %v, want nil", got)
	}
}

func TestErrorString(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "known without op", err: newError("", 31), want: "file read failed"},
		{name: "known with op", err: newError("Read", 28), want: "Read: file open failed"},
		{name: "unknown", err: newError("Open", 12345), want: "Open: unknown error code 12345"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.err.Error(); got != test.want {
				t.Fatalf("Error() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestErrorIsComparesCodes(t *testing.T) {
	if !errors.Is(&Error{Code: 30, Op: "Write"}, ErrFileWrite) {
		t.Fatal("fresh error with code 30 did not match ErrFileWrite")
	}
	if errors.Is(&Error{Code: 31}, ErrFileWrite) {
		t.Fatal("different error codes matched")
	}
	if errors.Is(&Error{Code: 4}, ErrClosed) {
		t.Fatal("TsFile error unexpectedly matched ErrClosed")
	}
	if !errors.Is(ErrClosed, ErrClosed) {
		t.Fatal("ErrClosed did not match itself")
	}
}
