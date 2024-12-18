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

#include "path_visitor.h"

namespace storage
{
    antlrcpp::Any PathVisitor::visitPath(PathParser::PathContext *ctx) {
        if (ctx->prefixPath() != nullptr) {
            return visitPrefixPath(ctx->prefixPath());
        } else {
            return visitSuffixPath(ctx->suffixPath());
        }
    } 

    antlrcpp::Any PathVisitor::visitPrefixPath(PathParser::PrefixPathContext *ctx) {
        std::vector<PathParser::NodeNameContext *> node_names = ctx->nodeName();
        std::vector<std::string> path;
        path.reserve(node_names.size() + 1);
        path.push_back(ctx->ROOT()->getText());
        for (uint64_t i = 0; i < node_names.size(); i++) {
            path.push_back(parse_node_name(node_names[i]));
        }
        return path;
    } 

    antlrcpp::Any PathVisitor::visitSuffixPath(PathParser::SuffixPathContext *ctx) {
        std::vector<PathParser::NodeNameContext *> node_names = ctx->nodeName();
        std::vector<std::string> path;
        path.reserve(node_names.size());
        for (uint64_t i = 0; i < node_names.size(); i++) {
            path.emplace_back(parse_node_name(node_names[i]));
        }
        return path;
    }
    std::string PathVisitor::parse_node_name(PathParser::NodeNameContext *ctx) {
        std::string node_name = ctx->getText();
        if (starts_with(node_name, BACK_QUOTE_STRING) && ends_with(node_name, BACK_QUOTE_STRING)) {
            std::string unWrapped = node_name.substr(1, node_name.length() - 2);
            if (is_real_number(unWrapped) || !std::regex_match(unWrapped, IDENTIFIER_PATTERN)) {
                return node_name;
            }
            
            return unWrapped;
        }

        return node_name;
    }
    bool PathVisitor::is_real_number(const std::string& str) {
        std::string s = str;
        if (starts_with(s, "+") || starts_with(s, "-")) {
            std::string removeSign = s.substr(1);
            if (starts_with(removeSign, "+") || starts_with(removeSign, "-")) {
                return false;
            } else {
                s = removeSign;
            }
        }
        size_t index = 0;
        auto it = std::find_if(s.begin(), s.end(), [](const char& c) { return c != '0'; });
        if (it != s.end()) {
            index = it - s.begin();
        }

        if (index > 0 && (s[index] == 'e' || s[index] == 'E')) {
            return is_creatable(s.substr(index - 1));
        } else {
            return is_creatable(s.substr(index));
        }
    }
    bool PathVisitor::starts_with(const std::string& str, const std::string& prefix) {
        if (prefix.size() > str.size()) {
            return false;
        }
        return str.substr(0, prefix.size()) == prefix;
    }

    bool PathVisitor::ends_with(const std::string& str, const std::string& suffix) {
        if (suffix.size() > str.size()) {
            return false;
        }
        return str.substr(str.size() - suffix.size()) == suffix;
    }

    bool PathVisitor::is_creatable(const std::string& str) {
        try {
            std::stod(str);
            return true;
        } catch (const std::invalid_argument& e) {
            return false;
        } catch (const std::out_of_range& e) {
            return false;
        }
    }
} // namespace storage
