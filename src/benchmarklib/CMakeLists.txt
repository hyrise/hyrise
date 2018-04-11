set(
    SOURCES

    benchmark_utilities/abstract_benchmark_table_generator.hpp
    benchmark_utilities/random_generator.hpp

    tpch/tpch_queries.cpp
    tpch/tpch_queries.hpp
    tpch/tpch_db_generator.cpp
    tpch/tpch_db_generator.hpp
)


# Configure the regular opossum library used for tests/server/playground...
add_library(hyriseBenchmarkLib STATIC ${SOURCES})
target_link_libraries(hyriseBenchmarkLib hyrise tpch_dbgen)
