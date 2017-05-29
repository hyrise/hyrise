add_executable(
    opossumBenchmark

    base_fixture.cpp
    benchmark_main.cpp
    benchmark_template.cpp
    operators/difference_benchmark.cpp
    operators/product_benchmark.cpp
    operators/projection_benchmark.cpp
    operators/sort_benchmark.cpp
    operators/table_scan_benchmark.cpp
    operators/union_all_benchmark.cpp
    table_generator.cpp
    table_generator.hpp
)

target_link_libraries(
    opossumBenchmark
    opossum
    benchmark
)