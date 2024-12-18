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


#include "PathParserListener.h"
#include "PathParserVisitor.h"

#include "PathParser.h"


using namespace antlrcpp;
using namespace antlr4;

PathParser::PathParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

PathParser::~PathParser() {
  delete _interpreter;
}

std::string PathParser::getGrammarFileName() const {
  return "PathParser.g4";
}

const std::vector<std::string>& PathParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& PathParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- PathContext ------------------------------------------------------------------

PathParser::PathContext::PathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PathParser::PrefixPathContext* PathParser::PathContext::prefixPath() {
  return getRuleContext<PathParser::PrefixPathContext>(0);
}

tree::TerminalNode* PathParser::PathContext::EOF() {
  return getToken(PathParser::EOF, 0);
}

PathParser::SuffixPathContext* PathParser::PathContext::suffixPath() {
  return getRuleContext<PathParser::SuffixPathContext>(0);
}


size_t PathParser::PathContext::getRuleIndex() const {
  return PathParser::RulePath;
}

void PathParser::PathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPath(this);
}

void PathParser::PathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPath(this);
}


antlrcpp::Any PathParser::PathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitPath(this);
  else
    return visitor->visitChildren(this);
}

PathParser::PathContext* PathParser::path() {
  PathContext *_localctx = _tracker.createInstance<PathContext>(_ctx, getState());
  enterRule(_localctx, 0, PathParser::RulePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(22);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PathParser::ROOT: {
        enterOuterAlt(_localctx, 1);
        setState(16);
        prefixPath();
        setState(17);
        match(PathParser::EOF);
        break;
      }

      case PathParser::STAR:
      case PathParser::DOUBLE_STAR:
      case PathParser::DURATION_LITERAL:
      case PathParser::INTEGER_LITERAL:
      case PathParser::ID:
      case PathParser::QUOTED_ID: {
        enterOuterAlt(_localctx, 2);
        setState(19);
        suffixPath();
        setState(20);
        match(PathParser::EOF);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrefixPathContext ------------------------------------------------------------------

PathParser::PrefixPathContext::PrefixPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PathParser::PrefixPathContext::ROOT() {
  return getToken(PathParser::ROOT, 0);
}

std::vector<tree::TerminalNode *> PathParser::PrefixPathContext::DOT() {
  return getTokens(PathParser::DOT);
}

tree::TerminalNode* PathParser::PrefixPathContext::DOT(size_t i) {
  return getToken(PathParser::DOT, i);
}

std::vector<PathParser::NodeNameContext *> PathParser::PrefixPathContext::nodeName() {
  return getRuleContexts<PathParser::NodeNameContext>();
}

PathParser::NodeNameContext* PathParser::PrefixPathContext::nodeName(size_t i) {
  return getRuleContext<PathParser::NodeNameContext>(i);
}


size_t PathParser::PrefixPathContext::getRuleIndex() const {
  return PathParser::RulePrefixPath;
}

void PathParser::PrefixPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrefixPath(this);
}

void PathParser::PrefixPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrefixPath(this);
}


antlrcpp::Any PathParser::PrefixPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitPrefixPath(this);
  else
    return visitor->visitChildren(this);
}

PathParser::PrefixPathContext* PathParser::prefixPath() {
  PrefixPathContext *_localctx = _tracker.createInstance<PrefixPathContext>(_ctx, getState());
  enterRule(_localctx, 2, PathParser::RulePrefixPath);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(24);
    match(PathParser::ROOT);
    setState(29);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PathParser::DOT) {
      setState(25);
      match(PathParser::DOT);
      setState(26);
      nodeName();
      setState(31);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SuffixPathContext ------------------------------------------------------------------

PathParser::SuffixPathContext::SuffixPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<PathParser::NodeNameContext *> PathParser::SuffixPathContext::nodeName() {
  return getRuleContexts<PathParser::NodeNameContext>();
}

PathParser::NodeNameContext* PathParser::SuffixPathContext::nodeName(size_t i) {
  return getRuleContext<PathParser::NodeNameContext>(i);
}

std::vector<tree::TerminalNode *> PathParser::SuffixPathContext::DOT() {
  return getTokens(PathParser::DOT);
}

tree::TerminalNode* PathParser::SuffixPathContext::DOT(size_t i) {
  return getToken(PathParser::DOT, i);
}


size_t PathParser::SuffixPathContext::getRuleIndex() const {
  return PathParser::RuleSuffixPath;
}

void PathParser::SuffixPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSuffixPath(this);
}

void PathParser::SuffixPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSuffixPath(this);
}


antlrcpp::Any PathParser::SuffixPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitSuffixPath(this);
  else
    return visitor->visitChildren(this);
}

PathParser::SuffixPathContext* PathParser::suffixPath() {
  SuffixPathContext *_localctx = _tracker.createInstance<SuffixPathContext>(_ctx, getState());
  enterRule(_localctx, 4, PathParser::RuleSuffixPath);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(32);
    nodeName();
    setState(37);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PathParser::DOT) {
      setState(33);
      match(PathParser::DOT);
      setState(34);
      nodeName();
      setState(39);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NodeNameContext ------------------------------------------------------------------

PathParser::NodeNameContext::NodeNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<PathParser::WildcardContext *> PathParser::NodeNameContext::wildcard() {
  return getRuleContexts<PathParser::WildcardContext>();
}

PathParser::WildcardContext* PathParser::NodeNameContext::wildcard(size_t i) {
  return getRuleContext<PathParser::WildcardContext>(i);
}

PathParser::NodeNameSliceContext* PathParser::NodeNameContext::nodeNameSlice() {
  return getRuleContext<PathParser::NodeNameSliceContext>(0);
}

PathParser::NodeNameWithoutWildcardContext* PathParser::NodeNameContext::nodeNameWithoutWildcard() {
  return getRuleContext<PathParser::NodeNameWithoutWildcardContext>(0);
}


size_t PathParser::NodeNameContext::getRuleIndex() const {
  return PathParser::RuleNodeName;
}

void PathParser::NodeNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNodeName(this);
}

void PathParser::NodeNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNodeName(this);
}


antlrcpp::Any PathParser::NodeNameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitNodeName(this);
  else
    return visitor->visitChildren(this);
}

PathParser::NodeNameContext* PathParser::nodeName() {
  NodeNameContext *_localctx = _tracker.createInstance<NodeNameContext>(_ctx, getState());
  enterRule(_localctx, 6, PathParser::RuleNodeName);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(50);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(40);
      wildcard();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(41);
      wildcard();
      setState(42);
      nodeNameSlice();
      setState(44);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == PathParser::STAR

      || _la == PathParser::DOUBLE_STAR) {
        setState(43);
        wildcard();
      }
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(46);
      nodeNameSlice();
      setState(47);
      wildcard();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(49);
      nodeNameWithoutWildcard();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NodeNameWithoutWildcardContext ------------------------------------------------------------------

PathParser::NodeNameWithoutWildcardContext::NodeNameWithoutWildcardContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PathParser::IdentifierContext* PathParser::NodeNameWithoutWildcardContext::identifier() {
  return getRuleContext<PathParser::IdentifierContext>(0);
}


size_t PathParser::NodeNameWithoutWildcardContext::getRuleIndex() const {
  return PathParser::RuleNodeNameWithoutWildcard;
}

void PathParser::NodeNameWithoutWildcardContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNodeNameWithoutWildcard(this);
}

void PathParser::NodeNameWithoutWildcardContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNodeNameWithoutWildcard(this);
}


antlrcpp::Any PathParser::NodeNameWithoutWildcardContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitNodeNameWithoutWildcard(this);
  else
    return visitor->visitChildren(this);
}

PathParser::NodeNameWithoutWildcardContext* PathParser::nodeNameWithoutWildcard() {
  NodeNameWithoutWildcardContext *_localctx = _tracker.createInstance<NodeNameWithoutWildcardContext>(_ctx, getState());
  enterRule(_localctx, 8, PathParser::RuleNodeNameWithoutWildcard);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(52);
    identifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NodeNameSliceContext ------------------------------------------------------------------

PathParser::NodeNameSliceContext::NodeNameSliceContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PathParser::IdentifierContext* PathParser::NodeNameSliceContext::identifier() {
  return getRuleContext<PathParser::IdentifierContext>(0);
}

tree::TerminalNode* PathParser::NodeNameSliceContext::INTEGER_LITERAL() {
  return getToken(PathParser::INTEGER_LITERAL, 0);
}


size_t PathParser::NodeNameSliceContext::getRuleIndex() const {
  return PathParser::RuleNodeNameSlice;
}

void PathParser::NodeNameSliceContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNodeNameSlice(this);
}

void PathParser::NodeNameSliceContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNodeNameSlice(this);
}


antlrcpp::Any PathParser::NodeNameSliceContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitNodeNameSlice(this);
  else
    return visitor->visitChildren(this);
}

PathParser::NodeNameSliceContext* PathParser::nodeNameSlice() {
  NodeNameSliceContext *_localctx = _tracker.createInstance<NodeNameSliceContext>(_ctx, getState());
  enterRule(_localctx, 10, PathParser::RuleNodeNameSlice);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(56);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PathParser::DURATION_LITERAL:
      case PathParser::ID:
      case PathParser::QUOTED_ID: {
        enterOuterAlt(_localctx, 1);
        setState(54);
        identifier();
        break;
      }

      case PathParser::INTEGER_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(55);
        match(PathParser::INTEGER_LITERAL);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierContext ------------------------------------------------------------------

PathParser::IdentifierContext::IdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PathParser::IdentifierContext::DURATION_LITERAL() {
  return getToken(PathParser::DURATION_LITERAL, 0);
}

tree::TerminalNode* PathParser::IdentifierContext::ID() {
  return getToken(PathParser::ID, 0);
}

tree::TerminalNode* PathParser::IdentifierContext::QUOTED_ID() {
  return getToken(PathParser::QUOTED_ID, 0);
}


size_t PathParser::IdentifierContext::getRuleIndex() const {
  return PathParser::RuleIdentifier;
}

void PathParser::IdentifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIdentifier(this);
}

void PathParser::IdentifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIdentifier(this);
}


antlrcpp::Any PathParser::IdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitIdentifier(this);
  else
    return visitor->visitChildren(this);
}

PathParser::IdentifierContext* PathParser::identifier() {
  IdentifierContext *_localctx = _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 12, PathParser::RuleIdentifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(58);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << PathParser::DURATION_LITERAL)
      | (1ULL << PathParser::ID)
      | (1ULL << PathParser::QUOTED_ID))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WildcardContext ------------------------------------------------------------------

PathParser::WildcardContext::WildcardContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PathParser::WildcardContext::STAR() {
  return getToken(PathParser::STAR, 0);
}

tree::TerminalNode* PathParser::WildcardContext::DOUBLE_STAR() {
  return getToken(PathParser::DOUBLE_STAR, 0);
}


size_t PathParser::WildcardContext::getRuleIndex() const {
  return PathParser::RuleWildcard;
}

void PathParser::WildcardContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWildcard(this);
}

void PathParser::WildcardContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PathParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWildcard(this);
}


antlrcpp::Any PathParser::WildcardContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PathParserVisitor*>(visitor))
    return parserVisitor->visitWildcard(this);
  else
    return visitor->visitChildren(this);
}

PathParser::WildcardContext* PathParser::wildcard() {
  WildcardContext *_localctx = _tracker.createInstance<WildcardContext>(_ctx, getState());
  enterRule(_localctx, 14, PathParser::RuleWildcard);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(60);
    _la = _input->LA(1);
    if (!(_la == PathParser::STAR

    || _la == PathParser::DOUBLE_STAR)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> PathParser::_decisionToDFA;
atn::PredictionContextCache PathParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN PathParser::_atn;
std::vector<uint16_t> PathParser::_serializedATN;

std::vector<std::string> PathParser::_ruleNames = {
  "path", "prefixPath", "suffixPath", "nodeName", "nodeNameWithoutWildcard", 
  "nodeNameSlice", "identifier", "wildcard"
};

std::vector<std::string> PathParser::_literalNames = {
  "", "", "", "", "", "'-'", "'+'", "'/'", "'%'", "'=='", "'='", "'>'", 
  "'>='", "'<'", "'<='", "", "'&'", "'&&'", "'|'", "'||'", "'!'", "'.'", 
  "','", "';'", "'*'", "'**'", "'('", "')'", "'['", "']'", "'::'"
};

std::vector<std::string> PathParser::_symbolicNames = {
  "", "ROOT", "WS", "TIME", "TIMESTAMP", "MINUS", "PLUS", "DIV", "MOD", 
  "OPERATOR_DEQ", "OPERATOR_SEQ", "OPERATOR_GT", "OPERATOR_GTE", "OPERATOR_LT", 
  "OPERATOR_LTE", "OPERATOR_NEQ", "OPERATOR_BITWISE_AND", "OPERATOR_LOGICAL_AND", 
  "OPERATOR_BITWISE_OR", "OPERATOR_LOGICAL_OR", "OPERATOR_NOT", "DOT", "COMMA", 
  "SEMI", "STAR", "DOUBLE_STAR", "LR_BRACKET", "RR_BRACKET", "LS_BRACKET", 
  "RS_BRACKET", "DOUBLE_COLON", "STRING_LITERAL", "DURATION_LITERAL", "DATETIME_LITERAL", 
  "INTEGER_LITERAL", "EXPONENT_NUM_PART", "ID", "QUOTED_ID"
};

dfa::Vocabulary PathParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> PathParser::_tokenNames;

PathParser::Initializer::Initializer() {
	for (size_t i = 0; i < _symbolicNames.size(); ++i) {
		std::string name = _vocabulary.getLiteralName(i);
		if (name.empty()) {
			name = _vocabulary.getSymbolicName(i);
		}

		if (name.empty()) {
			_tokenNames.push_back("<INVALID>");
		} else {
      _tokenNames.push_back(name);
    }
	}

  static const uint16_t serializedATNSegment0[] = {
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
       0x3, 0x27, 0x41, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
       0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 
       0x7, 0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x3, 0x2, 0x3, 0x2, 
       0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x5, 0x2, 0x19, 0xa, 0x2, 
       0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x7, 0x3, 0x1e, 0xa, 0x3, 0xc, 0x3, 
       0xe, 0x3, 0x21, 0xb, 0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x7, 0x4, 
       0x26, 0xa, 0x4, 0xc, 0x4, 0xe, 0x4, 0x29, 0xb, 0x4, 0x3, 0x5, 0x3, 
       0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x2f, 0xa, 0x5, 0x3, 0x5, 0x3, 
       0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x35, 0xa, 0x5, 0x3, 0x6, 0x3, 
       0x6, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x3b, 0xa, 0x7, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x2, 0x2, 0xa, 0x2, 0x4, 0x6, 
       0x8, 0xa, 0xc, 0xe, 0x10, 0x2, 0x4, 0x4, 0x2, 0x22, 0x22, 0x26, 0x27, 
       0x3, 0x2, 0x1a, 0x1b, 0x2, 0x40, 0x2, 0x18, 0x3, 0x2, 0x2, 0x2, 0x4, 
       0x1a, 0x3, 0x2, 0x2, 0x2, 0x6, 0x22, 0x3, 0x2, 0x2, 0x2, 0x8, 0x34, 
       0x3, 0x2, 0x2, 0x2, 0xa, 0x36, 0x3, 0x2, 0x2, 0x2, 0xc, 0x3a, 0x3, 
       0x2, 0x2, 0x2, 0xe, 0x3c, 0x3, 0x2, 0x2, 0x2, 0x10, 0x3e, 0x3, 0x2, 
       0x2, 0x2, 0x12, 0x13, 0x5, 0x4, 0x3, 0x2, 0x13, 0x14, 0x7, 0x2, 0x2, 
       0x3, 0x14, 0x19, 0x3, 0x2, 0x2, 0x2, 0x15, 0x16, 0x5, 0x6, 0x4, 0x2, 
       0x16, 0x17, 0x7, 0x2, 0x2, 0x3, 0x17, 0x19, 0x3, 0x2, 0x2, 0x2, 0x18, 
       0x12, 0x3, 0x2, 0x2, 0x2, 0x18, 0x15, 0x3, 0x2, 0x2, 0x2, 0x19, 0x3, 
       0x3, 0x2, 0x2, 0x2, 0x1a, 0x1f, 0x7, 0x3, 0x2, 0x2, 0x1b, 0x1c, 0x7, 
       0x17, 0x2, 0x2, 0x1c, 0x1e, 0x5, 0x8, 0x5, 0x2, 0x1d, 0x1b, 0x3, 
       0x2, 0x2, 0x2, 0x1e, 0x21, 0x3, 0x2, 0x2, 0x2, 0x1f, 0x1d, 0x3, 0x2, 
       0x2, 0x2, 0x1f, 0x20, 0x3, 0x2, 0x2, 0x2, 0x20, 0x5, 0x3, 0x2, 0x2, 
       0x2, 0x21, 0x1f, 0x3, 0x2, 0x2, 0x2, 0x22, 0x27, 0x5, 0x8, 0x5, 0x2, 
       0x23, 0x24, 0x7, 0x17, 0x2, 0x2, 0x24, 0x26, 0x5, 0x8, 0x5, 0x2, 
       0x25, 0x23, 0x3, 0x2, 0x2, 0x2, 0x26, 0x29, 0x3, 0x2, 0x2, 0x2, 0x27, 
       0x25, 0x3, 0x2, 0x2, 0x2, 0x27, 0x28, 0x3, 0x2, 0x2, 0x2, 0x28, 0x7, 
       0x3, 0x2, 0x2, 0x2, 0x29, 0x27, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x35, 0x5, 
       0x10, 0x9, 0x2, 0x2b, 0x2c, 0x5, 0x10, 0x9, 0x2, 0x2c, 0x2e, 0x5, 
       0xc, 0x7, 0x2, 0x2d, 0x2f, 0x5, 0x10, 0x9, 0x2, 0x2e, 0x2d, 0x3, 
       0x2, 0x2, 0x2, 0x2e, 0x2f, 0x3, 0x2, 0x2, 0x2, 0x2f, 0x35, 0x3, 0x2, 
       0x2, 0x2, 0x30, 0x31, 0x5, 0xc, 0x7, 0x2, 0x31, 0x32, 0x5, 0x10, 
       0x9, 0x2, 0x32, 0x35, 0x3, 0x2, 0x2, 0x2, 0x33, 0x35, 0x5, 0xa, 0x6, 
       0x2, 0x34, 0x2a, 0x3, 0x2, 0x2, 0x2, 0x34, 0x2b, 0x3, 0x2, 0x2, 0x2, 
       0x34, 0x30, 0x3, 0x2, 0x2, 0x2, 0x34, 0x33, 0x3, 0x2, 0x2, 0x2, 0x35, 
       0x9, 0x3, 0x2, 0x2, 0x2, 0x36, 0x37, 0x5, 0xe, 0x8, 0x2, 0x37, 0xb, 
       0x3, 0x2, 0x2, 0x2, 0x38, 0x3b, 0x5, 0xe, 0x8, 0x2, 0x39, 0x3b, 0x7, 
       0x24, 0x2, 0x2, 0x3a, 0x38, 0x3, 0x2, 0x2, 0x2, 0x3a, 0x39, 0x3, 
       0x2, 0x2, 0x2, 0x3b, 0xd, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x3d, 0x9, 0x2, 
       0x2, 0x2, 0x3d, 0xf, 0x3, 0x2, 0x2, 0x2, 0x3e, 0x3f, 0x9, 0x3, 0x2, 
       0x2, 0x3f, 0x11, 0x3, 0x2, 0x2, 0x2, 0x8, 0x18, 0x1f, 0x27, 0x2e, 
       0x34, 0x3a, 
  };

  _serializedATN.insert(_serializedATN.end(), serializedATNSegment0,
    serializedATNSegment0 + sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));


  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

PathParser::Initializer PathParser::_init;
