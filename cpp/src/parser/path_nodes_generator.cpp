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
#include <string>
#include <vector>

#include "path_nodes_generator.h"
#include "utils/errno_define.h"
#include "generated/PathLexer.h"
#include "generated/PathParser.h"
#include "path_parser_error.h"
#include "path_visitor.h"

namespace storage {
    std::vector<std::string> PathNodesGenerator::invokeParser(const std::string& path) {
        antlr4::ANTLRInputStream inputStream(path);
        PathLexer lexer(&inputStream);
        lexer.removeErrorListeners(); 
        lexer.addErrorListener(&PathParseError::getInstance());
        antlr4::CommonTokenStream tokens(&lexer);
        PathParser parser(&tokens);
        parser.removeErrorListeners(); 
        parser.addErrorListener(&PathParseError::getInstance());
        parser.getInterpreter<antlr4::atn::ParserATNSimulator>()->setPredictionMode(antlr4::atn::PredictionMode::LL);
        /* if use SLL Mode to parse path, it will throw exception
            but c++ tsfile forbid throw exception, so we use LL Mode
            to parse path.
        */
        PathVisitor path_visitor;
        return path_visitor.visit(parser.path()).as<std::vector<std::string>>();
    }
}
