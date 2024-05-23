
message("Running in src/storage/tsfile directory")

aux_source_directory(. storage_tsfile_SRC_LIST)
aux_source_directory(filter storage_tsfile_filter_SRC_LIST)
set(storage_tsfile_INC_LIST )
set(storage_tsfile_DEFINE_OPTIONS )
set(storage_tsfile_COMPILE_OPTIONS )
set(storage_tsfile_LINK_OPTIONS )
set(storage_tsfile_for_server_SRC_LIST ${storage_tsfile_SRC_LIST}
                                       ${storage_tsfile_filter_SRC_LIST})

add_statis_objtarget(storage_tsfile storage_tsfile_for_server_SRC_LIST
                                    storage_tsfile_INC_LIST
                                 "${storage_tsfile_DEFINE_OPTIONS}"
                                 "${storage_tsfile_COMPILE_OPTIONS}"
                                 "${storage_tsfile_LINK_OPTIONS}")

add_subdirectory(compress)
add_subdirectory(encode)

# for libtsfile SDK
# FIXME remove elog
aux_source_directory(. libtsfile_SRC_LIST)
aux_source_directory(../common common_SRC_LIST)
aux_source_directory(../common/allocator common_allocator_SRC_LIST)
aux_source_directory(../common/container common_container_SRC_LIST)
aux_source_directory(../common/tsblock common_tsblock_SRC_LIST)
aux_source_directory(../common/logger common_logger_SRC_LIST)
aux_source_directory(. storage_tsfile_SRC_LIST)
aux_source_directory(compress storage_tsfile_compress_SRC_LIST)
set(libtsfile_tsfile_SRC_LIST tsfile_writer.cc
        qds_with_timegenerator.cc
        qds_without_timegenerator.cc
        tablet.cc
        qds_with_timegenerator.cc
        qds_without_timegenerator.cc
        tablet.cc
        tsfile_executor.cc
        tsfile_reader.cc)
set(libtsfile_DEP_SRC_LIST ${common_SRC_LIST}
                           ${common_allocator_SRC_LIST}
                           ${common_logger_SRC_LIST}
                           ${common_container_SRC_LIST}
                           ${common_tsblock_SRC_LIST}
                           ${storage_tsfile_SRC_LIST}
                           ${libtsfile_tsfile_SRC_LIST}
                           ${storage_tsfile_filter_SRC_LIST}
                           ${storage_tsfile_compress_SRC_LIST})
                           
add_library(tsfile SHARED ${libtsfile_SRC_LIST} ${libtsfile_DEP_SRC_LIST})

# set(CMAKE_PREFIX_PATH ../../third-party/zlib-1.2.13/install/lib)
# set(ZLIB_LIB_DIR ../../third-party/zlib-1.2.13/install/lib)
# # find_library(my_z_lib NAMES z PATHS ${ZLIB_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
# # target_link_libraries(tsfile ${my_z_lib})

# set(CMAKE_PREFIX_PATH ../../third-party/lz4-dev/lib)
# set(LZ4_LIB_DIR ../../third-party/lz4-dev/lib)
# find_library(my_lz4_lib NAMES lz4 PATHS ${LZ4_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
# link_directories(${LZ4_LIB_DIR})
# target_link_libraries(tsfile ${my_lz4_lib})

set(LIBTSFILE_PROJECT_VERSION 1.0)
set(LIBTSFILE_SO_VERSION 1.0)
set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})

set(LIBTSFILE_SDK_DIR ${EXECUTABLE_OUTPUT_PATH}/libtsfile_sdk/)
install(TARGETS tsfile LIBRARY DESTINATION ${LIBTSFILE_SDK_DIR}/lib)

set(libtsfile_tsfile_HEADER_FILES tsfile_reader.h
        schema.h
        record.h
        tsfile_writer.h
        tablet.h
        write_file.h
        read_file.h
        row_record.h
        path.h
        query_data_set.h
        expression.h)
install(FILES ${libtsfile_tsfile_HEADER_FILES} DESTINATION ${LIBTSFILE_SDK_DIR}/include/storage/tsfile)

set(libtsfile_common_HEADER_FILES ../common/db_common.h
        ../common/util_define.h
        ../common/errno_define.h
        ../../cmake/cmake_config.h)
install(FILES ${libtsfile_common_HEADER_FILES} DESTINATION ${LIBTSFILE_SDK_DIR}/include/common)

set(libtsfile_common_container_HEADER_FILES ../common/container/bit_map.h
        ../common/container/simple_vector.h)
install(FILES ${libtsfile_common_container_HEADER_FILES} DESTINATION ${LIBTSFILE_SDK_DIR}/include/common/container)

set(libtsfile_tsfile_filter_HEADER_FILES filter/time_filter.h
        filter/time_operator.h
        filter/and_filter.h
        filter/binary_filter.h
        filter/between.h
        filter/eq.h
        filter/filter.h
        filter/filter_type.h
        filter/gt_eq.h
        filter/gt.h
        filter/in.h
        filter/lt_eq.h
        filter/lt.h
        filter/not_eq.h
        filter/or_filter.h
        filter/time_filter.h
        filter/time_operator.h)
              
install(FILES ${libtsfile_tsfile_filter_HEADER_FILES} DESTINATION ${LIBTSFILE_SDK_DIR}/include/storage/tsfile/filter)

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep liblz4 | sed 's/dylib.*/dylib/g'` ${my_lz4_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
  add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep libz | sed 's/dylib.*/dylib/g'` ${my_z_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
endif()
