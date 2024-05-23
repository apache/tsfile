message("running in src/fuke diectory")

message("CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} file_SRC_LIST)
add_library(file_obj OBJECT ${file_SRC_LIST})