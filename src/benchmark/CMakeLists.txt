add_executable(
    hyriseBenchmark

    benchmark_basic_fixture.cpp
    benchmark_basic_fixture.hpp
    benchmark_join_fixture.cpp
    benchmark_join_fixture.hpp
    benchmark_main.cpp
    benchmark_template.cpp
    operators/aggregate_benchmark.cpp
    operators/difference_benchmark.cpp
    operators/join_benchmark.cpp
    operators/product_benchmark.cpp
    operators/projection_benchmark.cpp
    operators/union_positions_benchmark.cpp
    operators/sort_benchmark.cpp
    operators/sql_benchmark.cpp
    operators/table_scan_benchmark.cpp
    operators/union_all_benchmark.cpp
    table_generator.cpp
    table_generator.hpp
    tpch_db_generator_benchmark.cpp
)

target_link_libraries(
    hyriseBenchmark
    hyrise
    hyriseBenchmarkLib
    benchmark
)

add_executable(
    hyriseBenchmarkTPCC

    benchmark_main.cpp
    benchmark_template.cpp
    tpcc/delivery_benchmark.cpp
    tpcc/new_order_benchmark.cpp
    tpcc/order_status_benchmark.cpp
    tpcc/tpcc_base_fixture.cpp
    tpcc/tpcc_base_fixture.hpp
)

target_link_libraries(
    hyriseBenchmarkTPCC
    hyrise
    hyriseBenchmarkLib
    benchmark
)


# Configure hyriseBenchmarkTPCH
add_executable(hyriseBenchmarkTPCH tpch_benchmark.cpp)
target_link_libraries(
    hyriseBenchmarkTPCH
    hyrise
    hyriseBenchmarkLib
)

# Configure hyriseEncodedColumnBenchmark
add_executable(hyriseEncodedColumnBenchmark
    encoded_columns/abstract_column_benchmark.cpp
    encoded_columns/abstract_column_benchmark.hpp
    encoded_columns/benchmark_main.cpp
    encoded_columns/benchmark_memory_resource.cpp
    encoded_columns/benchmark_memory_resource.hpp
    encoded_columns/benchmark_state.cpp
    encoded_columns/benchmark_state.hpp
    encoded_columns/clear_cache.cpp
    encoded_columns/clear_cache.hpp
    encoded_columns/from_string.hpp
    encoded_columns/generate_calibration_description.cpp
    encoded_columns/generate_calibration_description.hpp
    encoded_columns/multi_distribution_column_benchmark.cpp
    encoded_columns/multi_distribution_column_benchmark.hpp
    encoded_columns/single_distribution_benchmark.cpp
    encoded_columns/single_distribution_benchmark.hpp
    encoded_columns/to_string.hpp
)

target_link_libraries(
    hyriseEncodedColumnBenchmark
    hyrise
    hyriseBenchmarkLib
)
