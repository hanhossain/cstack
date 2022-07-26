cmake_minimum_required(VERSION 3.22)
project(cstack C)

set(CMAKE_C_STANDARD 23)

add_subdirectory(cstacksys)

add_executable(cstack main.c cstacksys/cstacksys.h)
target_link_libraries(cstack ${CMAKE_CURRENT_BINARY_DIR}/cstacksys/libcstacksys.a)
add_dependencies(cstack cstacksys)
