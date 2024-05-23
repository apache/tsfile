message("Running in cwrapper directory")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
aux_source_directory(. CWRAPPER_SRC_LIST)
add_library(cwrapper_obj OBJECT ${CWRAPPER_SRC_LIST})