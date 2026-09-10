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
	"strings"
)

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

// ColumnMetadata describes one column in a query result. The timestamp is the
// first column.
type ColumnMetadata struct {
	Name     string
	DataType DataType
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
		DataTypeBlob, DataTypeString:
		return true
	default:
		return false
	}
}

// validateAndCopyTableSchema returns an independently owned, normalized
// schema because Writer retains it after NewWriter returns. Mutating the
// caller's Columns slice must not change later Tablet validation.
func validateAndCopyTableSchema(schema TableSchema) (TableSchema, error) {
	if err := validateCString("validate table schema", "table name", schema.Table); err != nil {
		return TableSchema{}, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}
	if len(schema.Columns) == 0 {
		return TableSchema{}, fmt.Errorf("%w: table requires at least one column", ErrInvalidSchema)
	}
	normalized := TableSchema{Table: normalizeIdentifier(schema.Table), Columns: append([]ColumnSchema(nil), schema.Columns...)}
	seen := make(map[string]struct{}, len(normalized.Columns))
	for i, column := range normalized.Columns {
		if err := validateCString("validate table schema", "column name", column.Name); err != nil {
			return TableSchema{}, fmt.Errorf("%w: column %d: %v", ErrInvalidSchema, i, err)
		}
		name := normalizeIdentifier(column.Name)
		if _, ok := seen[name]; ok {
			return TableSchema{}, fmt.Errorf("%w: duplicate column %q", ErrInvalidSchema, column.Name)
		}
		seen[name] = struct{}{}
		if !validDataType(column.DataType) {
			return TableSchema{}, fmt.Errorf("%w: unsupported data type %d for column %q", ErrInvalidSchema, column.DataType, column.Name)
		}
		if column.Category != ColumnCategoryTag && column.Category != ColumnCategoryField {
			return TableSchema{}, fmt.Errorf("%w: unsupported category %d for column %q", ErrInvalidSchema, column.Category, column.Name)
		}
		if column.Category == ColumnCategoryTag && column.DataType != DataTypeString {
			return TableSchema{}, fmt.Errorf("%w: TAG column %q must use STRING", ErrInvalidSchema, column.Name)
		}
		normalized.Columns[i].Name = name
	}
	return normalized, nil
}

func normalizeIdentifier(value string) string { return strings.ToLower(value) }
