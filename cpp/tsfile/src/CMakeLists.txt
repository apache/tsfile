message("Running in src diectory")
include(${PROJECT_SOURCE_DIR}/cmake/build_function.cmake)
add_subdirectory(tsfile)
add_subdirectory(common)