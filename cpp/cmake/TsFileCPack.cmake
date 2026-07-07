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

set(TSFILE_ROOT_DIR ${CMAKE_SOURCE_DIR}/..)
set(TSFILE_DOC_FILES "")
foreach(doc_file LICENSE NOTICE)
    if (EXISTS ${TSFILE_ROOT_DIR}/${doc_file})
        list(APPEND TSFILE_DOC_FILES ${TSFILE_ROOT_DIR}/${doc_file})
    endif()
endforeach()

if (TSFILE_DOC_FILES)
    install(FILES ${TSFILE_DOC_FILES}
            DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/libtsfile
            COMPONENT runtime)
    install(FILES ${TSFILE_DOC_FILES}
            DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/libtsfile-dev
            COMPONENT development)
    install(FILES ${TSFILE_DOC_FILES}
            DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/tsfile-cli
            COMPONENT tools)
endif()

set(CPACK_PACKAGE_NAME "apache-tsfile")
set(CPACK_PACKAGE_VENDOR "Apache Software Foundation")
set(CPACK_PACKAGE_CONTACT "Apache TsFile Developers <dev@tsfile.apache.org>")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://tsfile.apache.org/")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Apache TsFile C++ library and command-line tools")
set(CPACK_PACKAGE_VERSION ${TsFile_CPP_VERSION})
if (TsFile_CPP_VERSION MATCHES "^([0-9]+)\\.([0-9]+)\\.([0-9]+)")
    set(CPACK_PACKAGE_VERSION_MAJOR ${CMAKE_MATCH_1})
    set(CPACK_PACKAGE_VERSION_MINOR ${CMAKE_MATCH_2})
    set(CPACK_PACKAGE_VERSION_PATCH ${CMAKE_MATCH_3})
endif()
set(CPACK_PACKAGE_FILE_NAME
        "apache-tsfile-${CPACK_PACKAGE_VERSION}-${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")
if (EXISTS ${TSFILE_ROOT_DIR}/LICENSE)
    set(CPACK_RESOURCE_FILE_LICENSE ${TSFILE_ROOT_DIR}/LICENSE)
endif()

set(CPACK_COMPONENTS_ALL runtime development tools)
set(CPACK_COMPONENT_RUNTIME_DISPLAY_NAME "libtsfile")
set(CPACK_COMPONENT_RUNTIME_DESCRIPTION "Apache TsFile shared library.")
set(CPACK_COMPONENT_DEVELOPMENT_DISPLAY_NAME "libtsfile development files")
set(CPACK_COMPONENT_DEVELOPMENT_DESCRIPTION
        "Apache TsFile C/C++ headers and CMake package files.")
set(CPACK_COMPONENT_DEVELOPMENT_DEPENDS runtime)
set(CPACK_COMPONENT_TOOLS_DISPLAY_NAME "tsfile-cli")
set(CPACK_COMPONENT_TOOLS_DESCRIPTION "Apache TsFile command-line tool.")
set(CPACK_COMPONENT_TOOLS_DEPENDS runtime)

set(CPACK_GENERATOR "TGZ")
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND CPACK_GENERATOR "DEB" "RPM")
endif()
set(CPACK_PACKAGING_INSTALL_PREFIX "/usr")

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_ENABLE_COMPONENT_DEPENDS ON)
set(CPACK_DEBIAN_FILE_NAME "DEB-DEFAULT")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER ${CPACK_PACKAGE_CONTACT})
set(CPACK_DEBIAN_RUNTIME_PACKAGE_NAME "libtsfile")
set(CPACK_DEBIAN_RUNTIME_PACKAGE_SECTION "libs")
set(CPACK_DEBIAN_RUNTIME_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_DEVELOPMENT_PACKAGE_NAME "libtsfile-dev")
set(CPACK_DEBIAN_DEVELOPMENT_PACKAGE_SECTION "libdevel")
set(CPACK_DEBIAN_TOOLS_PACKAGE_NAME "tsfile-cli")
set(CPACK_DEBIAN_TOOLS_PACKAGE_SECTION "database")

set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_FILE_NAME "RPM-DEFAULT")
set(CPACK_RPM_PACKAGE_LICENSE "Apache-2.0")
set(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
set(CPACK_RPM_RUNTIME_PACKAGE_NAME "libtsfile")
set(CPACK_RPM_RUNTIME_PACKAGE_GROUP "System Environment/Libraries")
set(CPACK_RPM_DEVELOPMENT_PACKAGE_NAME "libtsfile-devel")
set(CPACK_RPM_DEVELOPMENT_PACKAGE_GROUP "Development/Libraries")
set(CPACK_RPM_TOOLS_PACKAGE_NAME "tsfile-cli")
set(CPACK_RPM_TOOLS_PACKAGE_GROUP "Applications/Databases")

include(CPack)
