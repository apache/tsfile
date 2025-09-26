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
 * This interface defines an abstract listener for a parse tree produced by
 * PathParser.
 */
class PathParserListener : public antlr4::tree::ParseTreeListener {
   public:
    virtual void enterPath(PathParser::PathContext *ctx) = 0;
    virtual void exitPath(PathParser::PathContext *ctx) = 0;

    virtual void enterPrefixPath(PathParser::PrefixPathContext *ctx) = 0;
    virtual void exitPrefixPath(PathParser::PrefixPathContext *ctx) = 0;

    virtual void enterSuffixPath(PathParser::SuffixPathContext *ctx) = 0;
    virtual void exitSuffixPath(PathParser::SuffixPathContext *ctx) = 0;

    virtual void enterNodeName(PathParser::NodeNameContext *ctx) = 0;
    virtual void exitNodeName(PathParser::NodeNameContext *ctx) = 0;

    virtual void enterNodeNameWithoutWildcard(
        PathParser::NodeNameWithoutWildcardContext *ctx) = 0;
    virtual void exitNodeNameWithoutWildcard(
        PathParser::NodeNameWithoutWildcardContext *ctx) = 0;

    virtual void enterNodeNameSlice(PathParser::NodeNameSliceContext *ctx) = 0;
    virtual void exitNodeNameSlice(PathParser::NodeNameSliceContext *ctx) = 0;

    virtual void enterIdentifier(PathParser::IdentifierContext *ctx) = 0;
    virtual void exitIdentifier(PathParser::IdentifierContext *ctx) = 0;

    virtual void enterWildcard(PathParser::WildcardContext *ctx) = 0;
    virtual void exitWildcard(PathParser::WildcardContext *ctx) = 0;
};
