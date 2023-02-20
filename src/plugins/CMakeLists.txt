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

    # We tried to link both libraries directly to hyrise, which resulted in segmentation faults when the
    # "ENABLE_COVERAGE" compiler flag is set.
    target_link_libraries_system(
        ${PARSED_ARGS_NAME}

        tbb
        ${BOOST_LIBS}
    )

    foreach(dep ${PARSED_ARGS_DEPS})
        target_link_libraries(${PARSED_ARGS_NAME} PRIVATE ${dep})
    endforeach(dep)

    # Prevent the linker under macOS from complaining about undefined methods
    if (APPLE)
        if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
            target_link_libraries(${PARSED_ARGS_NAME} PRIVATE "-undefined dynamic_lookup")
        endif()
    endif()
endfunction(add_plugin)

set(
    DISCOVERY_PLUGIN_SOURCES
    dependency_discovery_plugin.cpp
    dependency_discovery_plugin.hpp
    dependency_discovery/dependency_candidates.cpp
    dependency_discovery/dependency_candidates.hpp
    dependency_discovery/candidate_strategy/abstract_dependency_candidate_rule.hpp
    dependency_discovery/candidate_strategy/dependent_group_by_reduction_candidate_rule.cpp
    dependency_discovery/candidate_strategy/dependent_group_by_reduction_candidate_rule.hpp
    dependency_discovery/candidate_strategy/join_to_predicate_candidate_rule.cpp
    dependency_discovery/candidate_strategy/join_to_predicate_candidate_rule.hpp
    dependency_discovery/candidate_strategy/join_to_semi_join_candidate_rule.cpp
    dependency_discovery/candidate_strategy/join_to_semi_join_candidate_rule.hpp
    dependency_discovery/validation_strategy/abstract_dependency_validation_rule.cpp
    dependency_discovery/validation_strategy/abstract_dependency_validation_rule.hpp
)

add_plugin(NAME hyriseMvccDeletePlugin SRCS mvcc_delete_plugin.cpp mvcc_delete_plugin.hpp DEPS gtest hyriseBenchmarkLib magic_enum sqlparser)
add_plugin(NAME hyriseSecondTestPlugin SRCS second_test_plugin.cpp second_test_plugin.hpp DEPS hyriseBenchmarkLib magic_enum sqlparser)
add_plugin(NAME hyriseTestNonInstantiablePlugin SRCS non_instantiable_plugin.cpp DEPS hyriseBenchmarkLib)
add_plugin(NAME hyriseTestPlugin SRCS test_plugin.cpp test_plugin.hpp DEPS hyriseBenchmarkLib magic_enum sqlparser)
add_plugin(NAME hyriseDependencyDiscoveryPlugin SRCS ${DISCOVERY_PLUGIN_SOURCES} DEPS compact_vector hyriseBenchmarkLib magic_enum sqlparser)

# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes.
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")
