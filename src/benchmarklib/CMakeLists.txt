include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/third_party
)

# The TPC-DS implementation redefines macros and structs with great liberty.
# That breaks with unity builds, so we turn them off here.
set(CMAKE_UNITY_BUILD "OFF")

set(
    SOURCES

    tpcc/constants.hpp
    tpcc/defines.hpp
    tpcc/tpcc_benchmark_item_runner.cpp
    tpcc/tpcc_benchmark_item_runner.hpp
    tpcc/tpcc_random_generator.hpp
    tpcc/tpcc_table_generator.cpp
    tpcc/tpcc_table_generator.hpp
    tpcc/procedures/abstract_tpcc_procedure.cpp
    tpcc/procedures/abstract_tpcc_procedure.hpp
    tpcc/procedures/tpcc_delivery.cpp
    tpcc/procedures/tpcc_delivery.hpp
    tpcc/procedures/tpcc_new_order.cpp
    tpcc/procedures/tpcc_new_order.hpp
    tpcc/procedures/tpcc_order_status.cpp
    tpcc/procedures/tpcc_order_status.hpp
    tpcc/procedures/tpcc_payment.cpp
    tpcc/procedures/tpcc_payment.hpp
    tpcc/procedures/tpcc_stock_level.cpp
    tpcc/procedures/tpcc_stock_level.hpp

    tpch/tpch_constants.hpp
    tpch/tpch_queries.cpp
    tpch/tpch_queries.hpp
    tpch/tpch_benchmark_item_runner.cpp
    tpch/tpch_benchmark_item_runner.hpp
    tpch/tpch_table_generator.cpp
    tpch/tpch_table_generator.hpp

    jcch/jcch_benchmark_item_runner.cpp
    jcch/jcch_benchmark_item_runner.hpp
    jcch/jcch_table_generator.cpp
    jcch/jcch_table_generator.hpp

    tpcds/tpcds_table_generator.cpp
    tpcds/tpcds_table_generator.hpp

    ssb/ssb_table_generator.cpp
    ssb/ssb_table_generator.hpp

    abstract_table_generator.cpp
    abstract_table_generator.hpp
    abstract_benchmark_item_runner.hpp
    abstract_benchmark_item_runner.cpp
    benchmark_config.cpp
    benchmark_config.hpp
    benchmark_item_result.cpp
    benchmark_item_result.hpp
    benchmark_item_run_result.cpp
    benchmark_item_run_result.hpp
    benchmark_runner.cpp
    benchmark_runner.hpp
    benchmark_sql_executor.cpp
    benchmark_sql_executor.hpp
    benchmark_state.cpp
    benchmark_state.hpp
    benchmark_table_encoder.cpp
    benchmark_table_encoder.hpp
    cli_config_parser.cpp
    cli_config_parser.hpp
    encoding_config.cpp
    encoding_config.hpp
    external_dbgen_utils.hpp
    external_dbgen_utils.cpp
    file_based_benchmark_item_runner.cpp
    file_based_benchmark_item_runner.hpp
    file_based_table_generator.cpp
    file_based_table_generator.hpp
    random_generator.hpp
    table_builder.hpp
    synthetic_table_generator.cpp
    synthetic_table_generator.hpp
)

# Configure the regular hyrise library used for tests/server/playground.
add_library(hyriseBenchmarkLib STATIC ${SOURCES})
add_dependencies(hyriseBenchmarkLib jcchDbgen-build) # *-build is auto-generated
add_dependencies(hyriseBenchmarkLib ssbDbgen-build) # *-build is auto-generated

# -fPIC generates position independent code which is necessary because plugins can access AbstractBenchmarkItemRunner in their pre-benchmark hooks.
target_compile_options(hyriseBenchmarkLib PRIVATE -fPIC)

target_link_libraries(
    hyriseBenchmarkLib
    PUBLIC

    hyrise
    ${Boost_DATE_TIME_LIBRARY}
    tpch_dbgen
    tpcds_dbgen
)
