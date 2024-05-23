message("running in src/write diectory")

message("CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} write_SRC_LIST)
add_library(write_obj OBJECT ${write_SRC_LIST})