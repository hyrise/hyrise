include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(
    SOURCES

    tpcc/constants.hpp
    tpcc/defines.hpp
    tpcc/helper.hpp
    tpcc/helper.cpp
    tpcc/tpcc_random_generator.hpp
    tpcc/tpcc_table_generator.cpp
    tpcc/tpcc_table_generator.hpp

    tpch/tpch_queries.cpp
    tpch/tpch_queries.hpp
    tpch/tpch_query_generator.cpp
    tpch/tpch_query_generator.hpp
    tpch/tpch_table_generator.cpp
    tpch/tpch_table_generator.hpp

    abstract_benchmark_table_generator.hpp
    abstract_table_generator.cpp
    abstract_table_generator.hpp
    abstract_query_generator.hpp
    abstract_query_generator.cpp
    benchmark_config.cpp
    benchmark_config.hpp
    benchmark_runner.cpp
    benchmark_runner.hpp
    benchmark_state.cpp
    benchmark_state.hpp
    benchmark_table_encoder.cpp
    benchmark_table_encoder.hpp
    cli_config_parser.cpp
    cli_config_parser.hpp
    encoding_config.cpp
    encoding_config.hpp
    file_based_table_generator.cpp
    file_based_table_generator.hpp
    file_based_query_generator.cpp
    file_based_query_generator.hpp
    table_generator.cpp
    table_generator.hpp
    random_generator.hpp
    query_benchmark_result.cpp
    query_benchmark_result.hpp
)


# Configure the regular opossum library used for tests/server/playground...
add_library(hyriseBenchmarkLib STATIC ${SOURCES})

target_link_libraries(
    hyriseBenchmarkLib
    hyrise
    tpch_dbgen)
