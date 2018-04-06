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
