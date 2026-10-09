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

// TagFilter is an immutable predicate over table TAG columns.
type TagFilter interface {
	tagFilter()
}

type tagComparisonOp uint8

const (
	tagEqual tagComparisonOp = iota
	tagNotEqual
	tagLess
	tagLessEqual
	tagGreater
	tagGreaterEqual
	tagBetween
	tagAnd
	tagOr
	tagNot
)

type tagFilter struct {
	op          tagComparisonOp
	column      string
	value       string
	secondValue string
	left        TagFilter
	right       TagFilter
}

func (*tagFilter) tagFilter() {}

func Eq(column, value string) TagFilter {
	return &tagFilter{op: tagEqual, column: normalizeIdentifier(column), value: value}
}
func Neq(column, value string) TagFilter {
	return &tagFilter{op: tagNotEqual, column: normalizeIdentifier(column), value: value}
}
func Lt(column, value string) TagFilter {
	return &tagFilter{op: tagLess, column: normalizeIdentifier(column), value: value}
}
func Lte(column, value string) TagFilter {
	return &tagFilter{op: tagLessEqual, column: normalizeIdentifier(column), value: value}
}
func Gt(column, value string) TagFilter {
	return &tagFilter{op: tagGreater, column: normalizeIdentifier(column), value: value}
}
func Gte(column, value string) TagFilter {
	return &tagFilter{op: tagGreaterEqual, column: normalizeIdentifier(column), value: value}
}
func Between(column, lower, upper string) TagFilter {
	return &tagFilter{op: tagBetween, column: normalizeIdentifier(column), value: lower, secondValue: upper}
}
func And(left, right TagFilter) TagFilter { return &tagFilter{op: tagAnd, left: left, right: right} }
func Or(left, right TagFilter) TagFilter  { return &tagFilter{op: tagOr, left: left, right: right} }
func Not(filter TagFilter) TagFilter      { return &tagFilter{op: tagNot, left: filter} }

func validateTagFilter(filter TagFilter) error {
	value, ok := filter.(*tagFilter)
	if !ok || value == nil {
		return fmt.Errorf("%w: invalid tag filter", ErrInvalidArgument)
	}
	switch value.op {
	case tagEqual, tagNotEqual, tagLess, tagLessEqual, tagGreater, tagGreaterEqual:
		return validateTagComparison(value.column, value.value)
	case tagBetween:
		if err := validateTagComparison(value.column, value.value); err != nil {
			return err
		}
		if strings.IndexByte(value.secondValue, 0) >= 0 || value.value > value.secondValue {
			return fmt.Errorf("%w: invalid tag range", ErrInvalidArgument)
		}
		return nil
	case tagAnd, tagOr:
		if err := validateTagFilter(value.left); err != nil {
			return err
		}
		return validateTagFilter(value.right)
	case tagNot:
		return validateTagFilter(value.left)
	default:
		return fmt.Errorf("%w: invalid tag filter operation", ErrInvalidArgument)
	}
}

func validateTagComparison(column, value string) error {
	if column == "" || strings.IndexByte(column, 0) >= 0 || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: tag column must be non-empty and strings must not contain NUL", ErrInvalidArgument)
	}
	return nil
}
