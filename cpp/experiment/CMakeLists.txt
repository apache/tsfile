cmake_minimum_required(VERSION 3.10)
message("Running in experiment directory")

add_subdirectory(write_memory)
add_subdirectory(read_memory)
add_subdirectory(read_perf)
add_subdirectory(thread_bench)
add_subdirectory(mem_constrained)
