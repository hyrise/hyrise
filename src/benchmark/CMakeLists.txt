add_executable(
    opossumBenchmark
    operators/projection_benchmark.cpp
    operators/union_all_benchmark.cpp
    operators/sort_benchmark.cpp
    operators/table_scan_benchmark.cpp
    operators/product_benchmark.cpp
    operators/difference_benchmark.cpp
    benchmark_main.cpp
    base_fixture.cpp
    table_generator.cpp
    table_generator.hpp
    benchmark_template.cpp
)

target_link_libraries(
    opossumBenchmark
    opossum
    benchmark
)

add_executable(
        opossumBenchmarkTPCC
        tpcc/delivery_benchmark.cpp
        tpcc/order_status_benchmark.cpp
        tpcc/new_order_benchmark.cpp
        tpcc/payment_benchmark.cpp
        tpcc/tpcc_base_fixture.cpp
        benchmark_main.cpp
        benchmark_template.cpp
)

target_link_libraries(
        opossumBenchmarkTPCC
        opossum
        benchmark
        tpcc
)
