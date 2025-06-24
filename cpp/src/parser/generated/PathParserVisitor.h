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
// Generated from PathParser.g4 by ANTLR 4.9.3

#pragma once

#include "PathParser.h"
#include "antlr4-runtime.h"

/**
 * This class defines an abstract visitor for a parse tree
 * produced by PathParser.
 */
class PathParserVisitor : public antlr4::tree::AbstractParseTreeVisitor {
   public:
    /**
     * Visit parse trees produced by PathParser.
     */
    virtual antlrcpp::Any visitPath(PathParser::PathContext *context) = 0;

    virtual antlrcpp::Any visitPrefixPath(
        PathParser::PrefixPathContext *context) = 0;

    virtual antlrcpp::Any visitSuffixPath(
        PathParser::SuffixPathContext *context) = 0;

    virtual antlrcpp::Any visitNodeName(
        PathParser::NodeNameContext *context) = 0;

    virtual antlrcpp::Any visitNodeNameWithoutWildcard(
        PathParser::NodeNameWithoutWildcardContext *context) = 0;

    virtual antlrcpp::Any visitNodeNameSlice(
        PathParser::NodeNameSliceContext *context) = 0;

    virtual antlrcpp::Any visitIdentifier(
        PathParser::IdentifierContext *context) = 0;

    virtual antlrcpp::Any visitWildcard(
        PathParser::WildcardContext *context) = 0;
};
