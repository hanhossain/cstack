cmake_minimum_required(VERSION 3.22)
project(cstack C)

set(CMAKE_C_STANDARD 23)

add_executable(cstack main.c cstacksys/cstacksys.h)
target_link_libraries(cstack ${CMAKE_SOURCE_DIR}/cstacksys/target/debug/libcstacksys.a)
