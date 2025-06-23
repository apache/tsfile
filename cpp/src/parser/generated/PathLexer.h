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
// Generated from PathLexer.g4 by ANTLR 4.9.3

#pragma once

#include "antlr4-runtime.h"

class PathLexer : public antlr4::Lexer {
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

    explicit PathLexer(antlr4::CharStream* input);
    ~PathLexer();

    virtual std::string getGrammarFileName() const override;
    virtual const std::vector<std::string>& getRuleNames() const override;

    virtual const std::vector<std::string>& getChannelNames() const override;
    virtual const std::vector<std::string>& getModeNames() const override;
    virtual const std::vector<std::string>& getTokenNames()
        const override;  // deprecated, use vocabulary instead
    virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

    virtual const std::vector<uint16_t> getSerializedATN() const override;
    virtual const antlr4::atn::ATN& getATN() const override;

   private:
    static std::vector<antlr4::dfa::DFA> _decisionToDFA;
    static antlr4::atn::PredictionContextCache _sharedContextCache;
    static std::vector<std::string> _ruleNames;
    static std::vector<std::string> _tokenNames;
    static std::vector<std::string> _channelNames;
    static std::vector<std::string> _modeNames;

    static std::vector<std::string> _literalNames;
    static std::vector<std::string> _symbolicNames;
    static antlr4::dfa::Vocabulary _vocabulary;
    static antlr4::atn::ATN _atn;
    static std::vector<uint16_t> _serializedATN;

    // Individual action functions triggered by action() above.

    // Individual semantic predicate functions triggered by sempred() above.

    struct Initializer {
        Initializer();
    };
    static Initializer _init;
};
