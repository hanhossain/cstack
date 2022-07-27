cmake_minimum_required(VERSION 3.22)
project(cstack C)

set(CMAKE_C_STANDARD 23)

include(FetchContent)

FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.2.1
)

FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(MANIFEST_PATH cstacksys/Cargo.toml)

add_executable(cstack main.c cstacksys/cstacksys.h)
target_link_libraries(cstack PUBLIC cstacksys)
add_dependencies(cstack cstacksys)
