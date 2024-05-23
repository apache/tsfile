message("Running in src diectory")
include(${PROJECT_SOURCE_DIR}/cmake/build_function.cmake)

add_subdirectory(common)
add_subdirectory(compress)
add_subdirectory(cwrapper)
add_subdirectory(encoding)
add_subdirectory(file)
add_subdirectory(reader)
add_subdirectory(utils)
add_subdirectory(writer)

add_library(tsfile SHARED)
target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj )

set(LIBTSFILE_PROJECT_VERSION 1.0)
set(LIBTSFILE_SO_VERSION 1.0)
set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})

set(LIBTSFILE_SDK_DIR ${EXECUTABLE_OUTPUT_PATH}/libtsfile_sdk/)
install(TARGETS tsfile LIBRARY DESTINATION ${LIBTSFILE_SDK_DIR}/lib)

# set(CMAKE_PREFIX_PATH ../../third-party/zlib-1.2.13/install/lib)
# set(ZLIB_LIB_DIR ../../third-party/zlib-1.2.13/install/lib)
# # find_library(my_z_lib NAMES z PATHS ${ZLIB_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
# # target_link_libraries(tsfile ${my_z_lib})

# set(CMAKE_PREFIX_PATH ../../third-party/lz4-dev/lib)
# set(LZ4_LIB_DIR ../../third-party/lz4-dev/lib)
# find_library(my_lz4_lib NAMES lz4 PATHS ${LZ4_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
# link_directories(${LZ4_LIB_DIR})
# target_link_libraries(tsfile ${my_lz4_lib})

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep liblz4 | sed 's/dylib.*/dylib/g'` ${my_lz4_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
  add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep libz | sed 's/dylib.*/dylib/g'` ${my_z_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
endif()