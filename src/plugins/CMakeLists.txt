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

add_plugin(NAME hyriseMvccDeletePlugin SRCS mvcc_delete_plugin.cpp mvcc_delete_plugin.hpp DEPS sqlparser magic_enum gtest)
add_plugin(NAME hyriseSecondTestPlugin SRCS second_test_plugin.cpp second_test_plugin.hpp DEPS sqlparser)
add_plugin(NAME hyriseTestPlugin SRCS test_plugin.cpp test_plugin.hpp DEPS sqlparser)
add_plugin(NAME hyriseTestNonInstantiablePlugin SRCS non_instantiable_plugin.cpp)

add_plugin(NAME hyriseFeatureExtractionPlugin
  SRCS
    feature_extraction_plugin.cpp
    feature_extraction_plugin.hpp
    feature_extraction/feature_extraction_utils.cpp
    feature_extraction/feature_extraction_utils.hpp
    feature_extraction/feature_nodes/abstract_feature_node.cpp
    feature_extraction/feature_nodes/abstract_feature_node.hpp
    feature_extraction/feature_nodes/aggregate_function_feature_node.cpp
    feature_extraction/feature_nodes/aggregate_function_feature_node.hpp
    feature_extraction/feature_nodes/abstract_table_feature_node.cpp
    feature_extraction/feature_nodes/abstract_table_feature_node.hpp
    feature_extraction/feature_nodes/base_table_feature_node.cpp
    feature_extraction/feature_nodes/base_table_feature_node.hpp
    feature_extraction/feature_nodes/column_feature_node.cpp
    feature_extraction/feature_nodes/column_feature_node.hpp
    feature_extraction/feature_nodes/operator_feature_node.cpp
    feature_extraction/feature_nodes/operator_feature_node.hpp
    feature_extraction/feature_nodes/predicate_feature_node.cpp
    feature_extraction/feature_nodes/predicate_feature_node.hpp
    feature_extraction/feature_nodes/result_table_feature_node.cpp
    feature_extraction/feature_nodes/result_table_feature_node.hpp
    feature_extraction/feature_nodes/segment_feature_node.cpp
    feature_extraction/feature_nodes/segment_feature_node.hpp
    feature_extraction/feature_types.cpp
    feature_extraction/feature_types.hpp
    feature_extraction/plan_exporter.cpp
    feature_extraction/plan_exporter.hpp
    feature_extraction/query_exporter.cpp
    feature_extraction/query_exporter.hpp
    feature_extraction/statistics_exporter.cpp
    feature_extraction/statistics_exporter.hpp

  DEPS
    magic_enum
    nlohmann_json
    sqlparser
    uninitialized_vector
)

target_link_libraries_system(
    hyriseFeatureExtractionPlugin

    flat-hash-map
    robin_map
)

add_plugin(NAME hyrisePhysicalConfigurationPlugin
  SRCS
    physical_configuration_plugin.cpp
    physical_configuration_plugin.hpp
    physical_configuration/physical_config.cpp
    physical_configuration/physical_config.hpp

  DEPS
    magic_enum
    nlohmann_json
    sqlparser
)

# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")
