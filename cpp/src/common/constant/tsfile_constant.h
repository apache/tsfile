/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef COMMON_CONSTANT_TSFILE_CONSTANT_H_
#define COMMON_CONSTANT_TSFILE_CONSTANT_H_
#include <regex>
#include <string>

namespace storage {
static const std::string TSFILE_SUFFIX = ".tsfile";
static const std::string TSFILE_HOME = "TSFILE_HOME";
static const std::string TSFILE_CONF = "TSFILE_CONF";
static const std::string PATH_ROOT = "root";
static const std::string TMP_SUFFIX = "tmp";
static const std::string PATH_SEPARATOR = ".";
static const char PATH_SEPARATOR_CHAR = '.';
static const std::string PATH_SEPARATER_NO_REGEX = "\\.";
static const char DOUBLE_QUOTE = '"';
static const char BACK_QUOTE = '`';
static const std::string BACK_QUOTE_STRING = "`";
static const std::string DOUBLE_BACK_QUOTE_STRING = "``";

static const unsigned char TIME_COLUMN_MASK = 0x80;
static const unsigned char VALUE_COLUMN_MASK = 0x40;

static const std::string TIME_COLUMN_ID = "";
static const int NO_STR_TO_READ = -1;

static const std::regex IDENTIFIER_PATTERN("([a-zA-Z0-9_\\u2E80-\\u9FFF]+)");
static const std::regex NODE_NAME_PATTERN(
    "(\\*{0,2}[a-zA-Z0-9_\\u2E80-\\u9FFF]+\\*{0,2})");
static const int DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME = 3;
}  // namespace storage

#endif
