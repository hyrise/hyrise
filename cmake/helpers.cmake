macro(add_asan_library arg1 arg2 arg3)
    add_library(${arg1} EXCLUDE_FROM_ALL STATIC ${arg2})
    target_link_libraries(${arg1} ${arg3} -fsanitize=address)
    target_include_directories(${arg1} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(${arg1} PROPERTIES COMPILE_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
endmacro()

macro(add_coverage_library arg1 arg2 arg3)
    add_library(${arg1} EXCLUDE_FROM_ALL STATIC ${arg2})
    target_link_libraries(
        ${arg1}
        ${arg3}
        --coverage
    )
    target_include_directories(${arg1} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(${arg1} PROPERTIES COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
endmacro()