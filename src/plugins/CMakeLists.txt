function(add_plugin)
    cmake_parse_arguments(
        PARSED_ARGS
        ""
        "NAME"
        "SRCS;DEPS"
        ${ARGN}
    )
    if(NOT PARSED_ARGS_NAME)
        message(FATAL_ERROR "You must provide a name for the plugin")
    endif(NOT PARSED_ARGS_NAME)

    add_library(${PARSED_ARGS_NAME} SHARED ${PARSED_ARGS_SRCS})

    foreach(dep ${PARSED_ARGS_DEPS})
        add_dependencies(${PARSED_ARGS_NAME} ${dep})
    endforeach(dep)

    # Prevent the linker under macOS from complaining about undefined methods
    if (APPLE)
        if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
            target_link_libraries(${PARSED_ARGS_NAME} PRIVATE "-undefined dynamic_lookup")
        endif()
    endif()
endfunction(add_plugin)

add_plugin(NAME TestPlugin SRCS test_plugin.cpp test_plugin.hpp)
add_plugin(NAME TestNonInstantiablePlugin SRCS non_instantiable_plugin.cpp)


# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")
