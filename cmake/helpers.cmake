set(SANITIZER_COMPILE_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
set(SANITIZER_LINK_FLAGS "-fsanitize=address")
set(COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")

macro(add_sanitizer_library arg1 arg2 arg3)
    add_library(${arg1} EXCLUDE_FROM_ALL STATIC ${arg2})
    target_link_libraries(${arg1} ${arg3} ${SANITIZER_LINK_FLAGS})
    target_include_directories(${arg1} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(${arg1} PROPERTIES COMPILE_FLAGS ${SANITIZER_COMPILE_FLAGS})
endmacro()

macro(add_sanitizer_executable arg1 arg2 arg3)
    add_executable(${arg1} EXCLUDE_FROM_ALL ${arg2})
    target_link_libraries(${arg1} ${arg3} ${SANITIZER_LINK_FLAGS})
    target_include_directories(${arg1} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(${arg1} PROPERTIES COMPILE_FLAGS ${SANITIZER_COMPILE_FLAGS})
endmacro()

macro(add_coverage_library arg1 arg2 arg3)
    add_library(${arg1} EXCLUDE_FROM_ALL STATIC ${arg2})
    target_link_libraries(
        ${arg1}
        ${arg3}
        --coverage
    )
    target_include_directories(${arg1} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(${arg1} PROPERTIES COMPILE_FLAGS ${COVERAGE_COMPILE_FLAGS})
endmacro()

macro(add_coverage_executable arg1 arg2 arg3)
    add_executable(${arg1} EXCLUDE_FROM_ALL ${arg2})
    target_link_libraries(
        ${arg1}
        ${arg3}
        --coverage
    )
    target_include_directories(${arg1} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(${arg1} PROPERTIES COMPILE_FLAGS ${COVERAGE_COMPILE_FLAGS})
endmacro()