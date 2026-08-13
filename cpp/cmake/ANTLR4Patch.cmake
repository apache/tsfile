#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
]]

if (NOT TSFILE_ANTLR4_RUNTIME_DIR)
    message(FATAL_ERROR "TSFILE_ANTLR4_RUNTIME_DIR must be provided.")
endif ()

function(_tsfile_patch_antlr4_file RELATIVE_PATH OLD_TEXT NEW_TEXT)
    set(_TSFILE_PATCH_FILE
            "${TSFILE_ANTLR4_RUNTIME_DIR}/src/${RELATIVE_PATH}")
    file(READ "${_TSFILE_PATCH_FILE}" _TSFILE_PATCH_CONTENT)
    string(FIND "${_TSFILE_PATCH_CONTENT}" "${OLD_TEXT}"
            _TSFILE_PATCH_POSITION)
    if (_TSFILE_PATCH_POSITION EQUAL -1)
        message(FATAL_ERROR
                "Cannot apply the TsFile ANTLR4 compatibility patch to "
                "${RELATIVE_PATH}: expected source text is missing.")
    endif ()
    string(REPLACE "${OLD_TEXT}" "${NEW_TEXT}"
            _TSFILE_PATCH_CONTENT "${_TSFILE_PATCH_CONTENT}")
    file(WRITE "${_TSFILE_PATCH_FILE}" "${_TSFILE_PATCH_CONTENT}")
endfunction()

_tsfile_patch_antlr4_file("RuleContext.h"
        [[bool operator == (const RuleContext &other) { return this == &other; }]]
        [[bool equal(const RuleContext &other) { return this == &other; }]])
_tsfile_patch_antlr4_file("Token.h"
        [[#if __cplusplus >= 201703L]]
        [[#if __cplusplus >= 201703L || defined(_MSC_VER)]])
_tsfile_patch_antlr4_file("Vocabulary.cpp"
        [[
#include "Token.h"]]
        [[
#include <locale>

#include "Token.h"]])
_tsfile_patch_antlr4_file("atn/ATN.cpp"
        [[expected.add(Token::EOF);]]
        [[expected.add(static_cast<int>(Token::EOF));]])
_tsfile_patch_antlr4_file("atn/LL1Analyzer.cpp"
        [[look.add(Token::EPSILON);]]
        [[look.add(static_cast<int>(Token::EPSILON));]])
_tsfile_patch_antlr4_file("atn/LL1Analyzer.cpp"
        [[look.add(Token::EOF);]]
        [[look.add(static_cast<int>(Token::EOF));]])
_tsfile_patch_antlr4_file("atn/LL1Analyzer.h"
        [[#if __cplusplus >= 201703L]]
        [[#if __cplusplus >= 201703L || defined(_MSC_VER)]])
_tsfile_patch_antlr4_file("atn/LexerATNSimulator.cpp"
        [[
LexerATNSimulator::SimState::~SimState() {
}
]]
        [[]])
_tsfile_patch_antlr4_file("atn/LexerATNSimulator.h"
        [[virtual ~SimState();]]
        [[virtual ~SimState() {(void)0;}]])
_tsfile_patch_antlr4_file("atn/LexerATNSimulator.h"
        [[virtual ~LexerATNSimulator () {}]]
        [[virtual ~LexerATNSimulator () {(void)0;}]])
_tsfile_patch_antlr4_file("misc/IntervalSet.cpp"
        [[_intervals = move(other._intervals);]]
        [[_intervals = std::move(other._intervals);]])
_tsfile_patch_antlr4_file("support/Any.h"
        [[template<int N = 0, typename std::enable_if<N == N && std::is_nothrow_copy_constructible<T>::value, int>::type = 0>]]
        [[template<typename U = T, typename std::enable_if<std::is_nothrow_copy_constructible<U>::value, int>::type = 0>]])
_tsfile_patch_antlr4_file("support/Any.h"
        [[template<int N = 0, typename std::enable_if<N == N && !std::is_nothrow_copy_constructible<T>::value, int>::type = 0>]]
        [[template<typename U = T, typename std::enable_if<!std::is_nothrow_copy_constructible<U>::value, int>::type = 0>]])
_tsfile_patch_antlr4_file("support/CPPUtils.cpp"
        [[          if (escapeSpaces) {
            result += "\u00B7";
            break;
          }
          // else fall through]]
        [[          if (escapeSpaces) {
            result += "\u00B7";
            break;
          } else {
            result += c;
            break;
          }
          // else fall through]])

function(_tsfile_verify_antlr4_patch RELATIVE_PATH EXPECTED_SHA256)
    file(SHA256 "${TSFILE_ANTLR4_RUNTIME_DIR}/src/${RELATIVE_PATH}"
            _TSFILE_PATCHED_SHA256)
    if (NOT _TSFILE_PATCHED_SHA256 STREQUAL EXPECTED_SHA256)
        message(FATAL_ERROR
                "The patched ANTLR4 file ${RELATIVE_PATH} does not match "
                "the reviewed TsFile compatibility patch set. Expected "
                "${EXPECTED_SHA256}, got ${_TSFILE_PATCHED_SHA256}.")
    endif ()
endfunction()

_tsfile_verify_antlr4_patch("RuleContext.h"
        "a2db34e609a614b55bae993a1d7a5eae319ef13e079de0b219f6989bc3cceacd")
_tsfile_verify_antlr4_patch("Token.h"
        "5963319fafbc3bd2d35d9726870e6be9e74f1111bfa993a79c97a5d47b85451d")
_tsfile_verify_antlr4_patch("Vocabulary.cpp"
        "9b774edfb8bcca7f869d17f1292ec38355bf5e1abc0c3e1fc582f917a74f6056")
_tsfile_verify_antlr4_patch("atn/ATN.cpp"
        "4fde921ac9c4e52327cfb7c2856b7786aaf1020b30a436ee61ca45597f07567e")
_tsfile_verify_antlr4_patch("atn/LL1Analyzer.cpp"
        "b4a26bba4fa7c8ff89aff9a8c374337523ff22346f275a01edc4fa855f212462")
_tsfile_verify_antlr4_patch("atn/LL1Analyzer.h"
        "a209e24d0c0ec304fe82a4fe6130d044ccd033927818c7e9372b76037caa91df")
_tsfile_verify_antlr4_patch("atn/LexerATNSimulator.cpp"
        "3b8fd401103ed111de680b9d0a7539d4e9d3654e47f92837aa11982dec5ed417")
_tsfile_verify_antlr4_patch("atn/LexerATNSimulator.h"
        "3d6b6f4b2a2ef5840fec226730b45faeaa9413ccf89b810d49dbf6308ea60e06")
_tsfile_verify_antlr4_patch("misc/IntervalSet.cpp"
        "8df38b75f0c9f84436c7066d708c2ee7af7a81e4ba6125c57b0d953e376b5cd0")
_tsfile_verify_antlr4_patch("support/Any.h"
        "23a2bbe88cea05d7e09263f5da5ba95d3e47f98c864e868a9116419e35f48c7b")
_tsfile_verify_antlr4_patch("support/CPPUtils.cpp"
        "4ef80e171fc581eb7276b53fb587a763cbd65621fbc615584d71fc183463c76d")
