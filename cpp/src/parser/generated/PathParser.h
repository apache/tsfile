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

#include "antlr4-runtime.h"

class PathParser : public antlr4::Parser {
   public:
    enum {
        ROOT = 1,
        WS = 2,
        TIME = 3,
        TIMESTAMP = 4,
        MINUS = 5,
        PLUS = 6,
        DIV = 7,
        MOD = 8,
        OPERATOR_DEQ = 9,
        OPERATOR_SEQ = 10,
        OPERATOR_GT = 11,
        OPERATOR_GTE = 12,
        OPERATOR_LT = 13,
        OPERATOR_LTE = 14,
        OPERATOR_NEQ = 15,
        OPERATOR_BITWISE_AND = 16,
        OPERATOR_LOGICAL_AND = 17,
        OPERATOR_BITWISE_OR = 18,
        OPERATOR_LOGICAL_OR = 19,
        OPERATOR_NOT = 20,
        DOT = 21,
        COMMA = 22,
        SEMI = 23,
        STAR = 24,
        DOUBLE_STAR = 25,
        LR_BRACKET = 26,
        RR_BRACKET = 27,
        LS_BRACKET = 28,
        RS_BRACKET = 29,
        DOUBLE_COLON = 30,
        STRING_LITERAL = 31,
        DURATION_LITERAL = 32,
        DATETIME_LITERAL = 33,
        INTEGER_LITERAL = 34,
        EXPONENT_NUM_PART = 35,
        ID = 36,
        QUOTED_ID = 37
    };

    enum {
        RulePath = 0,
        RulePrefixPath = 1,
        RuleSuffixPath = 2,
        RuleNodeName = 3,
        RuleNodeNameWithoutWildcard = 4,
        RuleNodeNameSlice = 5,
        RuleIdentifier = 6,
        RuleWildcard = 7
    };

    explicit PathParser(antlr4::TokenStream *input);
    ~PathParser();

    virtual std::string getGrammarFileName() const override;
    virtual const antlr4::atn::ATN &getATN() const override { return _atn; };
    virtual const std::vector<std::string> &getTokenNames() const override {
        return _tokenNames;
    };  // deprecated: use vocabulary instead.
    virtual const std::vector<std::string> &getRuleNames() const override;
    virtual antlr4::dfa::Vocabulary &getVocabulary() const override;

    class PathContext;
    class PrefixPathContext;
    class SuffixPathContext;
    class NodeNameContext;
    class NodeNameWithoutWildcardContext;
    class NodeNameSliceContext;
    class IdentifierContext;
    class WildcardContext;

    class PathContext : public antlr4::ParserRuleContext {
       public:
        PathContext(antlr4::ParserRuleContext *parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        PrefixPathContext *prefixPath();
        antlr4::tree::TerminalNode *EOF();
        SuffixPathContext *suffixPath();

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    PathContext *path();

    class PrefixPathContext : public antlr4::ParserRuleContext {
       public:
        PrefixPathContext(antlr4::ParserRuleContext *parent,
                          size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode *ROOT();
        std::vector<antlr4::tree::TerminalNode *> DOT();
        antlr4::tree::TerminalNode *DOT(size_t i);
        std::vector<NodeNameContext *> nodeName();
        NodeNameContext *nodeName(size_t i);

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    PrefixPathContext *prefixPath();

    class SuffixPathContext : public antlr4::ParserRuleContext {
       public:
        SuffixPathContext(antlr4::ParserRuleContext *parent,
                          size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<NodeNameContext *> nodeName();
        NodeNameContext *nodeName(size_t i);
        std::vector<antlr4::tree::TerminalNode *> DOT();
        antlr4::tree::TerminalNode *DOT(size_t i);

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    SuffixPathContext *suffixPath();

    class NodeNameContext : public antlr4::ParserRuleContext {
       public:
        NodeNameContext(antlr4::ParserRuleContext *parent,
                        size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<WildcardContext *> wildcard();
        WildcardContext *wildcard(size_t i);
        NodeNameSliceContext *nodeNameSlice();
        NodeNameWithoutWildcardContext *nodeNameWithoutWildcard();

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    NodeNameContext *nodeName();

    class NodeNameWithoutWildcardContext : public antlr4::ParserRuleContext {
       public:
        NodeNameWithoutWildcardContext(antlr4::ParserRuleContext *parent,
                                       size_t invokingState);
        virtual size_t getRuleIndex() const override;
        IdentifierContext *identifier();

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    NodeNameWithoutWildcardContext *nodeNameWithoutWildcard();

    class NodeNameSliceContext : public antlr4::ParserRuleContext {
       public:
        NodeNameSliceContext(antlr4::ParserRuleContext *parent,
                             size_t invokingState);
        virtual size_t getRuleIndex() const override;
        IdentifierContext *identifier();
        antlr4::tree::TerminalNode *INTEGER_LITERAL();

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    NodeNameSliceContext *nodeNameSlice();

    class IdentifierContext : public antlr4::ParserRuleContext {
       public:
        IdentifierContext(antlr4::ParserRuleContext *parent,
                          size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode *DURATION_LITERAL();
        antlr4::tree::TerminalNode *ID();
        antlr4::tree::TerminalNode *QUOTED_ID();

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    IdentifierContext *identifier();

    class WildcardContext : public antlr4::ParserRuleContext {
       public:
        WildcardContext(antlr4::ParserRuleContext *parent,
                        size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode *STAR();
        antlr4::tree::TerminalNode *DOUBLE_STAR();

        virtual void enterRule(
            antlr4::tree::ParseTreeListener *listener) override;
        virtual void exitRule(
            antlr4::tree::ParseTreeListener *listener) override;

        virtual antlrcpp::Any accept(
            antlr4::tree::ParseTreeVisitor *visitor) override;
    };

    WildcardContext *wildcard();

   private:
    static std::vector<antlr4::dfa::DFA> _decisionToDFA;
    static antlr4::atn::PredictionContextCache _sharedContextCache;
    static std::vector<std::string> _ruleNames;
    static std::vector<std::string> _tokenNames;

    static std::vector<std::string> _literalNames;
    static std::vector<std::string> _symbolicNames;
    static antlr4::dfa::Vocabulary _vocabulary;
    static antlr4::atn::ATN _atn;
    static std::vector<uint16_t> _serializedATN;

    struct Initializer {
        Initializer();
    };
    static Initializer _init;
};
