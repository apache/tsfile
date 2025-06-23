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

#ifndef PATH_VISITOR_H
#define PATH_VISITOR_H

#include "common/constant/tsfile_constant.h"
#include "generated/PathParser.h"
#include "generated/PathParserBaseVisitor.h"

namespace storage {
class PathVisitor : public PathParserBaseVisitor {
   public:
    antlrcpp::Any visitPath(PathParser::PathContext* ctx) override;

    antlrcpp::Any visitPrefixPath(PathParser::PrefixPathContext* ctx) override;

    antlrcpp::Any visitSuffixPath(PathParser::SuffixPathContext* ctx) override;

    static bool is_real_number(const std::string& str);

   private:
    std::string parse_node_name(PathParser::NodeNameContext* ctx);

    static bool starts_with(const std::string& src, const std::string& prefix);
    static bool ends_with(const std::string& src, const std::string& suffix);
    static bool is_creatable(const std::string& str);
};

}  // namespace storage

#endif
