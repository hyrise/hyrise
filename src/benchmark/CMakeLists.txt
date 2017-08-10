add_executable(
    opossumBenchmark

    base_fixture.cpp
    base_fixture.hpp
    benchmark_main.cpp
    benchmark_template.cpp
#    operators/aggregate_benchmark.cpp
#    operators/difference_benchmark.cpp
#    operators/product_benchmark.cpp
#    operators/projection_benchmark.cpp
#    operators/sort_benchmark.cpp
    operators/sql_benchmark.cpp
#    operators/table_scan_benchmark.cpp
#    operators/union_all_benchmark.cpp
    table_generator.cpp
    table_generator.hpp
)

target_link_libraries(
    opossumBenchmark
    opossum
    benchmark
)

add_executable(
    opossumBenchmarkTPCC

    benchmark_main.cpp
    benchmark_template.cpp
    tpcc/delivery_benchmark.cpp
    tpcc/new_order_benchmark.cpp
    tpcc/order_status_benchmark.cpp
    tpcc/tpcc_base_fixture.cpp
    tpcc/tpcc_base_fixture.hpp
)

target_link_libraries(
    opossumBenchmarkTPCC
    opossum
    benchmark
    tpcc
)
