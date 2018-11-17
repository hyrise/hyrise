include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(
    SOURCES

    benchmark_utilities/abstract_benchmark_table_generator.hpp
    benchmark_utilities/random_generator.hpp

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
    tpch/tpch_db_generator.cpp
    tpch/tpch_db_generator.hpp

    table_generator.cpp
    table_generator.hpp

    abstract_query_generator.cpp
    abstract_query_generator.hpp
    benchmark_runner.cpp
    benchmark_runner.hpp
    benchmark_utils.cpp
    benchmark_utils.hpp
)


# Configure the regular opossum library used for tests/server/playground...
add_library(hyriseBenchmarkLib STATIC ${SOURCES})

# Add this magic dependency to run the git_watcher on build
add_dependencies(hyriseBenchmarkLib AlwaysCheckGit)

target_link_libraries(hyriseBenchmarkLib hyrise tpch_dbgen ${FILESYSTEM_LIBRARY})
