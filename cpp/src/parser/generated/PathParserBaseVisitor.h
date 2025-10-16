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

#include "PathParserVisitor.h"
#include "antlr4-runtime.h"

/**
 * This class provides an empty implementation of PathParserVisitor, which can
 * be extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class PathParserBaseVisitor : public PathParserVisitor {
   public:
    virtual antlrcpp::Any visitPath(PathParser::PathContext *ctx) override {
        return visitChildren(ctx);
    }

    virtual antlrcpp::Any visitPrefixPath(
        PathParser::PrefixPathContext *ctx) override {
        return visitChildren(ctx);
    }

    virtual antlrcpp::Any visitSuffixPath(
        PathParser::SuffixPathContext *ctx) override {
        return visitChildren(ctx);
    }

    virtual antlrcpp::Any visitNodeName(
        PathParser::NodeNameContext *ctx) override {
        return visitChildren(ctx);
    }

    virtual antlrcpp::Any visitNodeNameWithoutWildcard(
        PathParser::NodeNameWithoutWildcardContext *ctx) override {
        return visitChildren(ctx);
    }

    virtual antlrcpp::Any visitNodeNameSlice(
        PathParser::NodeNameSliceContext *ctx) override {
        return visitChildren(ctx);
    }

    virtual antlrcpp::Any visitIdentifier(
        PathParser::IdentifierContext *ctx) override {
        return visitChildren(ctx);
    }

    virtual antlrcpp::Any visitWildcard(
        PathParser::WildcardContext *ctx) override {
        return visitChildren(ctx);
    }
};
