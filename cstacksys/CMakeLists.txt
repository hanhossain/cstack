# see https://michael-f-bryan.github.io/rust-ffi-guide/setting_up.html
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build)
    set(TARGET_DIR "debug")
else()
    set(CARGO_CMD cargo build --release)
    set(TARGET_DIR "release")
endif()

set(CSTACKSYS_A ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/libcstacksys.a)
add_custom_target(cstacksys ALL
    COMMENT "Compiling cstacksys module"
    COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} ${CARGO_CMD}
    COMMAND cp ${CSTACKSYS_A} ${CMAKE_CURRENT_BINARY_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(cstacksys PROPERTIES LOCATION ${CMAKE_CURRENT_BINARY_DIR})
