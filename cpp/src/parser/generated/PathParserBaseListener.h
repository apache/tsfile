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

#include "PathParserListener.h"
#include "antlr4-runtime.h"

/**
 * This class provides an empty implementation of PathParserListener,
 * which can be extended to create a listener which only needs to handle a
 * subset of the available methods.
 */
class PathParserBaseListener : public PathParserListener {
   public:
    virtual void enterPath(PathParser::PathContext* /*ctx*/) override {}
    virtual void exitPath(PathParser::PathContext* /*ctx*/) override {}

    virtual void enterPrefixPath(
        PathParser::PrefixPathContext* /*ctx*/) override {}
    virtual void exitPrefixPath(
        PathParser::PrefixPathContext* /*ctx*/) override {}

    virtual void enterSuffixPath(
        PathParser::SuffixPathContext* /*ctx*/) override {}
    virtual void exitSuffixPath(
        PathParser::SuffixPathContext* /*ctx*/) override {}

    virtual void enterNodeName(PathParser::NodeNameContext* /*ctx*/) override {}
    virtual void exitNodeName(PathParser::NodeNameContext* /*ctx*/) override {}

    virtual void enterNodeNameWithoutWildcard(
        PathParser::NodeNameWithoutWildcardContext* /*ctx*/) override {}
    virtual void exitNodeNameWithoutWildcard(
        PathParser::NodeNameWithoutWildcardContext* /*ctx*/) override {}

    virtual void enterNodeNameSlice(
        PathParser::NodeNameSliceContext* /*ctx*/) override {}
    virtual void exitNodeNameSlice(
        PathParser::NodeNameSliceContext* /*ctx*/) override {}

    virtual void enterIdentifier(
        PathParser::IdentifierContext* /*ctx*/) override {}
    virtual void exitIdentifier(
        PathParser::IdentifierContext* /*ctx*/) override {}

    virtual void enterWildcard(PathParser::WildcardContext* /*ctx*/) override {}
    virtual void exitWildcard(PathParser::WildcardContext* /*ctx*/) override {}

    virtual void enterEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
    virtual void exitEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
    virtual void visitTerminal(antlr4::tree::TerminalNode* /*node*/) override {}
    virtual void visitErrorNode(antlr4::tree::ErrorNode* /*node*/) override {}
};
