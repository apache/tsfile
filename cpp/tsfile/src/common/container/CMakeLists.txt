message("Running in src/common/container directory")

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_container_SRC_LIST)
set(common_container_INC_LIST)
set(common_container_DEFINE_OPTIONS)
set(common_container_COMPILE_OPTIONS)
set(common_container_LINK_OPTIONS)

add_statis_objtarget(common_container common_container_SRC_LIST common_container_INC_LIST
                     "${common_container_DEFINE_OPTIONS}"
                     "${common_container_COMPILE_OPTIONS}"
                     "${common_container_LINK_OPTIONS}")
