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
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>

#include "antlr4-runtime.h"

class PathParseError : public antlr4::BaseErrorListener {
   public:
    static PathParseError& getInstance() {
        static PathParseError instance;
        return instance;
    }

    void syntaxError(antlr4::Recognizer* recognizer,
                     antlr4::Token* offending_symbol, size_t line,
                     size_t char_position_inLine, const std::string& msg,
                     std::exception_ptr e) override {
        std::string modified_msg = msg;

        auto* parser = dynamic_cast<antlr4::Parser*>(recognizer);
        if (parser != nullptr) {
            auto expectedTokens = parser->getExpectedTokens();
            auto vocabulary = parser->getVocabulary();

            std::set<std::string> expectedTokenNames;
            for (auto token : expectedTokens.toSet()) {
                expectedTokenNames.insert(vocabulary.getDisplayName(token));
            }

            if (expectedTokenNames.count("ID") &&
                expectedTokenNames.count("QUOTED_ID")) {
                std::ostringstream expectedStr;
                expectedStr << "{ID, QUOTED_ID";

                if (expectedTokenNames.count("*") &&
                    expectedTokenNames.count("**")) {
                    expectedStr << ", *, **";
                }

                expectedStr << "}";
                modified_msg =
                    replace_substring(msg, expectedTokens.toString(vocabulary),
                                      expectedStr.str());
            }
        }

        throw std::runtime_error("line " + std::to_string(line) + ":" +
                                 std::to_string(char_position_inLine) + " " +
                                 modified_msg);
    }

   private:
    static std::string replace_substring(const std::string& source,
                                         const std::string& from,
                                         const std::string& to) {
        size_t start_pos = source.find(from);
        if (start_pos == std::string::npos) {
            return source;
        }
        return source.substr(0, start_pos) + to +
               source.substr(start_pos + from.length());
    }
};