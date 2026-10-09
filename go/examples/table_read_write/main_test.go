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
	"io"
	"os"
	"strings"
	"testing"
)

func TestExampleWritesTabletAndArrowRows(t *testing.T) {
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(originalDir) })

	originalStdout := os.Stdout
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writeEnd
	t.Cleanup(func() { os.Stdout = originalStdout })
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if err := writeEnd.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = originalStdout
	output, err := io.ReadAll(readEnd)
	if err != nil {
		t.Fatal(err)
	}
	if err := readEnd.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 15 {
		t.Fatalf("output has %d rows, want 15:\n%s", len(lines), output)
	}
	if lines[0] != "1 d1 10" || lines[14] != "15 d1 150" {
		t.Fatalf("unexpected first/last rows: %q / %q", lines[0], lines[14])
	}
}
