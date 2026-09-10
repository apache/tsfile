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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func validTestSchema() TableSchema {
	return TableSchema{Table: "metrics", Columns: []ColumnSchema{
		{Name: "device", DataType: DataTypeString, Category: ColumnCategoryTag},
		{Name: "value", DataType: DataTypeDouble, Category: ColumnCategoryField},
		{Name: "payload", DataType: DataTypeBlob, Category: ColumnCategoryField},
	}}
}

func TestTableSchemaValidationAndCopy(t *testing.T) {
	schema := validTestSchema()
	schema.Table = "Metrics"
	schema.Columns[0].Name = "Device"
	copy, err := validateAndCopyTableSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	schema.Table = "changed"
	schema.Columns[0].Name = "changed"
	if copy.Table != "metrics" || copy.Columns[0].Name != "device" {
		t.Fatalf("schema was not deeply copied: %+v", copy)
	}

	cases := []TableSchema{
		{},
		{Table: "metrics"},
		{Table: "bad\x00name", Columns: validTestSchema().Columns},
		{Table: "metrics", Columns: []ColumnSchema{
			{Name: "dup", DataType: DataTypeString, Category: ColumnCategoryTag},
			{Name: "DUP", DataType: DataTypeDouble, Category: ColumnCategoryField},
		}},
		{Table: "metrics", Columns: []ColumnSchema{
			{Name: "bad", DataType: DataType(6), Category: ColumnCategoryField},
		}},
		{Table: "metrics", Columns: []ColumnSchema{
			{Name: "bad", DataType: DataTypeDouble, Category: ColumnCategory(2)},
		}},
	}
	for _, schema := range cases {
		if _, err := validateAndCopyTableSchema(schema); !errors.Is(err, ErrInvalidSchema) &&
			!errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("schema %+v: error = %v", schema, err)
		}
	}
}

func TestQueryOptionDefaultsAndComposition(t *testing.T) {
	options, err := buildQueryOptions()
	if err != nil {
		t.Fatal(err)
	}
	if options.start != math.MinInt64 || options.end != math.MaxInt64 ||
		options.offset != 0 || options.limit != -1 || options.batchSize != 0 || options.tagFilter != nil {
		t.Fatalf("unexpected defaults: %+v", options)
	}

	filter := And(Eq("device", "d1"), Not(Eq("region", "west")))
	options, err = buildQueryOptions(
		WithTimeRange(10, 30),
		WithTagFilter(filter),
		WithOffset(1),
		WithLimit(2),
		WithBatchSize(1024),
	)
	if err != nil {
		t.Fatal(err)
	}
	if options.start != 10 || options.end != 30 || options.offset != 1 ||
		options.limit != 2 || options.batchSize != 1024 || options.tagFilter != filter {
		t.Fatalf("unexpected composed options: %+v", options)
	}
}

func TestQueryOptionValidation(t *testing.T) {
	cases := []QueryOption{
		nil,
		WithTimeRange(2, 1),
		WithOffset(-1),
		WithTagFilter(nil),
	}
	for _, option := range cases {
		if _, err := buildQueryOptions(option); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("option error = %v, want ErrInvalidArgument", err)
		}
	}
	options, err := buildQueryOptions(WithBatchSize(-1))
	if err != nil || options.batchSize != 0 {
		t.Fatalf("negative batch size should select row mode: %+v, %v", options, err)
	}
	options, err = buildQueryOptions(WithLimit(-2))
	if err != nil || options.limit != -1 {
		t.Fatalf("negative limit should mean unlimited: %+v, %v", options, err)
	}
}

func TestResultSetUsesOneBasedColumnsAndModes(t *testing.T) {
	row := &ResultSet{
		metadata: []ColumnMetadata{
			{Name: "time", DataType: DataTypeInt64},
			{Name: "value", DataType: DataTypeDouble},
		},
		current: true,
		mode:    resultModeRows,
	}
	if err := row.validateColumn(0, DataTypeInt64); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("column zero error = %v", err)
	}
	if err := row.validateColumn(1, DataTypeInt64); err != nil {
		t.Fatalf("column one error = %v", err)
	}
	if err := row.requireMode(resultModeBatches); !errors.Is(err, ErrWrongResultMode) {
		t.Fatalf("row/batch mode error = %v", err)
	}

	batch := &ResultSet{mode: resultModeBatches}
	if err := batch.requireMode(resultModeRows); !errors.Is(err, ErrWrongResultMode) {
		t.Fatalf("batch/row mode error = %v", err)
	}
}

func TestTagFilterShapeValidation(t *testing.T) {
	cases := []TagFilter{
		Eq("", "d1"),
		Eq("bad\x00name", "d1"),
		And(Eq("device", "d1"), nil),
		Between("device", "z", "a"),
	}
	for _, filter := range cases {
		if err := validateTagFilter(filter); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("filter %#v: error = %v", filter, err)
		}
	}
	if err := validateTagFilter(Or(Eq("device", "d1"), Neq("device", "d2"))); err != nil {
		t.Fatal(err)
	}
	if got := Eq("Device", "d1").(*tagFilter).column; got != "device" {
		t.Fatalf("normalized tag column = %q", got)
	}
}

func TestArrowSchemaValidation(t *testing.T) {
	bound := validTestSchema()
	valid := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "device", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "payload", Type: arrow.BinaryTypes.Binary},
	}, nil)
	if timeColumn, err := validateArrowSchema(valid, bound); err != nil || timeColumn != 0 {
		t.Fatalf("valid Arrow schema = %d, %v", timeColumn, err)
	}
	mixedCase := arrow.NewSchema([]arrow.Field{
		{Name: "Time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "Device", Type: arrow.BinaryTypes.String},
		{Name: "Value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "Payload", Type: arrow.BinaryTypes.Binary},
	}, nil)
	if timeColumn, err := validateArrowSchema(mixedCase, bound); err != nil || timeColumn != 0 {
		t.Fatalf("mixed-case Arrow schema = %d, %v", timeColumn, err)
	}

	cases := []*arrow.Schema{
		arrow.NewSchema(valid.Fields()[1:], nil),
		arrow.NewSchema([]arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Float64},
			{Name: "device", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			{Name: "payload", Type: arrow.BinaryTypes.Binary},
		}, nil),
		arrow.NewSchema([]arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "device", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64},
			{Name: "payload", Type: arrow.BinaryTypes.Binary},
		}, nil),
	}
	for _, schema := range cases {
		if _, err := validateArrowSchema(schema, bound); err == nil {
			t.Fatalf("invalid Arrow schema accepted: %s", schema)
		}
	}
}
