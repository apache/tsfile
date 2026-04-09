# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


class TagFilter:
    """Base class for tag filters used in table queries."""

    def __and__(self, other):
        return AndTagFilter(self, other)

    def __or__(self, other):
        return OrTagFilter(self, other)

    def __invert__(self):
        return NotTagFilter(self)


class ComparisonTagFilter(TagFilter):
    """A tag filter comparing a column to a value with a given operator."""

    # Operator constants matching TagFilterOp enum in C
    EQ = 0
    NEQ = 1
    LT = 2
    LTEQ = 3
    GT = 4
    GTEQ = 5
    REGEXP = 6
    NOT_REGEXP = 7

    def __init__(self, column_name: str, value: str, op: int):
        self.column_name = column_name
        self.value = value
        self.op = op

    def __repr__(self):
        op_names = {
            0: "==",
            1: "!=",
            2: "<",
            3: "<=",
            4: ">",
            5: ">=",
            6: "=~",
            7: "!~",
        }
        return (
            f"TagFilter({self.column_name} {op_names.get(self.op, '?')} {self.value!r})"
        )


class BetweenTagFilter(TagFilter):
    """A tag filter for range queries."""

    def __init__(self, column_name: str, lower: str, upper: str, is_not: bool = False):
        self.column_name = column_name
        self.lower = lower
        self.upper = upper
        self.is_not = is_not

    def __repr__(self):
        op = "NOT BETWEEN" if self.is_not else "BETWEEN"
        return f"TagFilter({self.column_name} {op} {self.lower!r} AND {self.upper!r})"


class AndTagFilter(TagFilter):
    """Logical AND of two tag filters."""

    def __init__(self, left: TagFilter, right: TagFilter):
        self.left = left
        self.right = right

    def __repr__(self):
        return f"({self.left} AND {self.right})"


class OrTagFilter(TagFilter):
    """Logical OR of two tag filters."""

    def __init__(self, left: TagFilter, right: TagFilter):
        self.left = left
        self.right = right

    def __repr__(self):
        return f"({self.left} OR {self.right})"


class NotTagFilter(TagFilter):
    """Logical NOT of a tag filter."""

    def __init__(self, filter: TagFilter):
        self.filter = filter

    def __repr__(self):
        return f"NOT({self.filter})"


# Convenience factory functions
def tag_eq(column_name: str, value: str) -> TagFilter:
    """Create a tag equality filter: column == value."""
    return ComparisonTagFilter(column_name, value, ComparisonTagFilter.EQ)


def tag_neq(column_name: str, value: str) -> TagFilter:
    """Create a tag inequality filter: column != value."""
    return ComparisonTagFilter(column_name, value, ComparisonTagFilter.NEQ)


def tag_lt(column_name: str, value: str) -> TagFilter:
    """Create a tag less-than filter: column < value."""
    return ComparisonTagFilter(column_name, value, ComparisonTagFilter.LT)


def tag_lteq(column_name: str, value: str) -> TagFilter:
    """Create a tag less-than-or-equal filter: column <= value."""
    return ComparisonTagFilter(column_name, value, ComparisonTagFilter.LTEQ)


def tag_gt(column_name: str, value: str) -> TagFilter:
    """Create a tag greater-than filter: column > value."""
    return ComparisonTagFilter(column_name, value, ComparisonTagFilter.GT)


def tag_gteq(column_name: str, value: str) -> TagFilter:
    """Create a tag greater-than-or-equal filter: column >= value."""
    return ComparisonTagFilter(column_name, value, ComparisonTagFilter.GTEQ)


def tag_regexp(column_name: str, pattern: str) -> TagFilter:
    """Create a tag regex match filter."""
    return ComparisonTagFilter(column_name, pattern, ComparisonTagFilter.REGEXP)


def tag_not_regexp(column_name: str, pattern: str) -> TagFilter:
    """Create a tag regex not-match filter."""
    return ComparisonTagFilter(column_name, pattern, ComparisonTagFilter.NOT_REGEXP)


def tag_between(column_name: str, lower: str, upper: str) -> TagFilter:
    """Create a tag BETWEEN filter: lower <= column <= upper."""
    return BetweenTagFilter(column_name, lower, upper, is_not=False)


def tag_not_between(column_name: str, lower: str, upper: str) -> TagFilter:
    """Create a tag NOT BETWEEN filter."""
    return BetweenTagFilter(column_name, lower, upper, is_not=True)
