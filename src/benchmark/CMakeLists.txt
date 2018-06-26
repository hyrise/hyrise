include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(
    hyriseMicroBenchmarks

    benchmark_basic_fixture.cpp
    benchmark_basic_fixture.hpp
    benchmark_main.cpp
    operators/aggregate_benchmark.cpp
    operators/difference_benchmark.cpp
    operators/join_benchmark.cpp
    operators/projection_benchmark.cpp
    operators/union_positions_benchmark.cpp
    operators/sort_benchmark.cpp
    operators/sql_benchmark.cpp
    operators/table_scan_benchmark.cpp
    operators/union_all_benchmark.cpp
    statistics/generate_table_statistics_benchmark.cpp
    tpch_db_generator_benchmark.cpp
)

target_link_libraries(
    hyriseMicroBenchmarks
    hyrise
    hyriseBenchmarkLib
    benchmark
)

add_executable(
    hyriseBenchmarkPlayground

    benchmark_playground.cpp
    benchmark_basic_fixture.cpp
    benchmark_basic_fixture.hpp
    benchmark_main.cpp
)
target_link_libraries(
    hyriseBenchmarkPlayground
    
    hyrise
    hyriseBenchmarkLib
    benchmark
)

# General purpose benchmark runner
add_executable(hyriseBenchmark benchmark_runner_main.cpp)
target_link_libraries(
    hyriseBenchmark

    hyrise
    hyriseBenchmarkLib
)

# Configure hyriseBenchmarkTPCH
add_executable(hyriseBenchmarkTPCH tpch_benchmark.cpp)
target_link_libraries(
    hyriseBenchmarkTPCH

    hyrise
    hyriseBenchmarkLib
)

# Link Time Optimization (LTO)
if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    if(NOT ${CMAKE_VERSION} VERSION_LESS "3.9.0")
        cmake_minimum_required(VERSION 3.9 FATAL_ERROR)
        include(CheckIPOSupported)
        check_ipo_supported()
        set_target_properties(hyriseBenchmarkTPCH PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
        add_definitions(-DWITH_LTO)
        message(STATUS "Building with Link-Time Optimization")
    endif()
endif()