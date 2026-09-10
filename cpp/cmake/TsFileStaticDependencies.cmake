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

# Static archives do not absorb their link dependencies. Keep the private
# build wrappers out of the export: install bundled archives and describe
# imported link-only targets, or rediscover system packages on the consumer.
# install(FILES) also works on CMake 3.11 for targets in another directory.
function(tsfile_install_static_dependency NAME SOURCE SYSTEM_TARGET FIND_CODE)
    if (NOT TARGET TsFile::${NAME})
        return()
    endif ()
    target_link_libraries(tsfile INTERFACE
            $<INSTALL_INTERFACE:$<LINK_ONLY:TsFile::Static_${NAME}>>)
    set(_CODE "${TSFILE_STATIC_DEPENDENCY_CODE}")
    if (SOURCE STREQUAL "BUNDLED")
        get_target_property(_TARGET TsFile::${NAME} INTERFACE_LINK_LIBRARIES)
        install(FILES $<TARGET_FILE:${_TARGET}>
                DESTINATION "${CMAKE_INSTALL_LIBDIR}/tsfile/$<CONFIG>"
                COMPONENT development)
        string(APPEND _CODE
                "if (NOT TARGET TsFile::Static_${NAME})\n"
                "    add_library(TsFile::Static_${NAME} STATIC IMPORTED)\n"
                "endif ()\n"
                "set_property(TARGET TsFile::Static_${NAME} APPEND PROPERTY IMPORTED_CONFIGURATIONS $<UPPER_CASE:$<CONFIG>>)\n"
                "set_target_properties(TsFile::Static_${NAME} PROPERTIES IMPORTED_LOCATION_$<UPPER_CASE:$<CONFIG>> \"\${CMAKE_CURRENT_LIST_DIR}/../../tsfile/$<CONFIG>/$<TARGET_FILE_NAME:${_TARGET}>\")\n")
    else ()
        string(APPEND _CODE "${FIND_CODE}\n"
                "if (NOT TARGET TsFile::Static_${NAME})\n"
                "    add_library(TsFile::Static_${NAME} INTERFACE IMPORTED)\n"
                "    set_target_properties(TsFile::Static_${NAME} PROPERTIES INTERFACE_LINK_LIBRARIES ${SYSTEM_TARGET})\n"
                "endif ()\n")
    endif ()
    set(TSFILE_STATIC_DEPENDENCY_CODE "${_CODE}" PARENT_SCOPE)
endfunction()

set(TSFILE_STATIC_DEPENDENCY_CODE "")
# Keep the build-side compatibility interval when a static consumer rediscovers
# ANTLR4. Explicit comparisons also support CMake 3.11 and packages that report
# ANTLR_VERSION instead of antlr4-runtime_VERSION.
string(CONFIGURE [=[
find_package(utf8cpp CONFIG QUIET)
find_package(antlr4-runtime CONFIG QUIET)
set(_TSFILE_ANTLR4_VERSION "${antlr4-runtime_VERSION}")
if ("${_TSFILE_ANTLR4_VERSION}" STREQUAL "" AND DEFINED ANTLR_VERSION)
    set(_TSFILE_ANTLR4_VERSION "${ANTLR_VERSION}")
endif ()
if (NOT antlr4-runtime_FOUND OR
        "${_TSFILE_ANTLR4_VERSION}" STREQUAL "" OR
        _TSFILE_ANTLR4_VERSION VERSION_LESS "@TSFILE_ANTLR4_MIN_VERSION@" OR
        NOT _TSFILE_ANTLR4_VERSION VERSION_LESS "@TSFILE_ANTLR4_NEXT_INCOMPATIBLE_VERSION@" OR
        NOT TARGET @TSFILE_ANTLR4_SYSTEM_TARGET@)
    set(TsFile_FOUND FALSE)
    set(TsFile_NOT_FOUND_MESSAGE
            "Static TsFile requires a compatible system ANTLR4 >=@TSFILE_ANTLR4_MIN_VERSION@ and <@TSFILE_ANTLR4_NEXT_INCOMPATIBLE_VERSION@ with target @TSFILE_ANTLR4_SYSTEM_TARGET@; reported version '${_TSFILE_ANTLR4_VERSION}'.")
    unset(_TSFILE_ANTLR4_VERSION)
    return()
endif ()
unset(_TSFILE_ANTLR4_VERSION)
]=] _TSFILE_FIND_STATIC_ANTLR4 @ONLY)
tsfile_install_static_dependency(ANTLR4 "${TSFILE_ANTLR4_SOURCE}"
        "${TSFILE_ANTLR4_SYSTEM_TARGET}"
        "${_TSFILE_FIND_STATIC_ANTLR4}")
unset(_TSFILE_FIND_STATIC_ANTLR4)
if (ENABLE_ANTLR4 AND TSFILE_ANTLR4_SOURCE STREQUAL "BUNDLED")
    if (WIN32)
        string(APPEND TSFILE_STATIC_DEPENDENCY_CODE
                "set_property(TARGET TsFile::Static_ANTLR4 PROPERTY INTERFACE_LINK_LIBRARIES ole32)\n")
    else ()
        if (APPLE)
            set(_PLATFORM_LIBRARY CoreFoundation)
        else ()
            set(_PLATFORM_LIBRARY uuid)
        endif ()
        string(APPEND TSFILE_STATIC_DEPENDENCY_CODE
                "find_library(_TSFILE_ANTLR4_PLATFORM_LIBRARY ${_PLATFORM_LIBRARY})\n"
                "if (NOT _TSFILE_ANTLR4_PLATFORM_LIBRARY)\n"
                "    set(TsFile_FOUND FALSE)\n"
                "    set(TsFile_NOT_FOUND_MESSAGE \"Static TsFile needs ${_PLATFORM_LIBRARY}\")\n"
                "    return()\n"
                "endif ()\n"
                "set_property(TARGET TsFile::Static_ANTLR4 PROPERTY INTERFACE_LINK_LIBRARIES \"\${_TSFILE_ANTLR4_PLATFORM_LIBRARY}\")\n")
        unset(_PLATFORM_LIBRARY)
    endif ()
endif ()
tsfile_install_static_dependency(Snappy "${TSFILE_SNAPPY_SOURCE}" Snappy::snappy
        "find_dependency(Snappy ${TSFILE_SNAPPY_MIN_VERSION} CONFIG)")
tsfile_install_static_dependency(LZ4 "${TSFILE_LZ4_SOURCE}" LZ4::LZ4
        "find_dependency(LZ4 ${TSFILE_LZ4_MIN_VERSION})")
if (ENABLE_LZ4 AND TSFILE_LZ4_SOURCE STREQUAL "SYSTEM")
    install(FILES "${CMAKE_SOURCE_DIR}/cmake/FindLZ4.cmake"
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TsFile"
            COMPONENT development)
endif ()
tsfile_install_static_dependency(LZOKAY "${TSFILE_LZOKAY_SOURCE}" lzokay::lzokay
        "find_dependency(lzokay ${TSFILE_LZOKAY_MIN_VERSION} CONFIG)")
tsfile_install_static_dependency(ZLIB "${TSFILE_ZLIB_SOURCE}" ZLIB::ZLIB
        "find_dependency(ZLIB ${TSFILE_ZLIB_MIN_VERSION})")
tsfile_install_static_dependency(ZSTD "${TSFILE_ZSTD_SOURCE}"
        "${TSFILE_ZSTD_SYSTEM_TARGET}"
        "find_dependency(zstd ${TSFILE_ZSTD_MIN_VERSION} CONFIG)")
tsfile_install_static_dependency(LibLZMA "${TSFILE_LIBLZMA_SOURCE}" LibLZMA::LibLZMA
        "find_dependency(LibLZMA ${TSFILE_LIBLZMA_MIN_VERSION})\nif (NOT TARGET LibLZMA::LibLZMA)\n    add_library(LibLZMA::LibLZMA INTERFACE IMPORTED)\n    set_target_properties(LibLZMA::LibLZMA PROPERTIES INTERFACE_LINK_LIBRARIES \"\${LIBLZMA_LIBRARIES}\")\nendif ()")

configure_file("${CMAKE_SOURCE_DIR}/cmake/TsFileStaticDependencies.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/TsFileStaticDependencies.cmake.in" @ONLY)
file(GENERATE
        OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/TsFileStaticDependencies-$<CONFIG>.cmake"
        INPUT "${CMAKE_CURRENT_BINARY_DIR}/TsFileStaticDependencies.cmake.in")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/TsFileStaticDependencies-$<CONFIG>.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TsFile"
        COMPONENT development)
unset(TSFILE_STATIC_DEPENDENCY_CODE)
