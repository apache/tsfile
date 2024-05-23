
message("Running in src/storage/tsfile/compress directory")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
aux_source_directory(. compress_SRC_LIST)
add_library(compress_obj OBJECT ${compress_SRC_LIST})
