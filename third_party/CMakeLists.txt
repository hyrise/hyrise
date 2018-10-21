# Build google benchmark
add_library(
    benchmark

    STATIC

    benchmark_fix/dummy.cc
    benchmark/src/benchmark.cc
    benchmark/src/benchmark_register.cc
    benchmark/src/colorprint.cc
    benchmark/src/commandlineflags.cc
    benchmark/src/complexity.cc
    benchmark/src/console_reporter.cc
    benchmark/src/counter.cc
    benchmark/src/csv_reporter.cc
    benchmark/src/json_reporter.cc
    benchmark/src/reporter.cc
    benchmark/src/sleep.cc
    benchmark/src/statistics.cc
    benchmark/src/string_util.cc
    benchmark/src/sysinfo.cc
    benchmark/src/timers.cc
)

target_compile_options(
    benchmark
    PRIVATE
    -O3 -std=c++11 -DHAVE_STD_REGEX -DNDEBUG
)

target_include_directories(
    benchmark
    PRIVATE
    benchmark/include
)


# Build sql-parser
add_library(
    sqlparser

    sql-parser/src/parser/bison_parser.cpp
    sql-parser/src/parser/flex_lexer.cpp
    sql-parser/src/sql/Expr.cpp
    sql-parser/src/sql/PrepareStatement.cpp
    sql-parser/src/sql/SQLStatement.cpp
    sql-parser/src/sql/statements.cpp
    sql-parser/src/SQLParser.cpp
    sql-parser/src/SQLParserResult.cpp
    sql-parser/src/util/sqlhelper.cpp
)

target_compile_options(
    sqlparser
    PRIVATE
    -std=c++1z
    -O3 -Wno-sign-compare
)

add_library(
    cqf

    cqf/src/cqf2.cpp
    cqf/src/cqf4.cpp
    cqf/src/cqf8.cpp
    cqf/src/cqf16.cpp
    cqf/src/cqf32.cpp
)

target_include_directories(
    cqf

    PUBLIC
    ${PROJECT_SOURCE_DIR}/third_party/cqf/include
)



if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    target_compile_options(sqlparser PRIVATE -Wno-unneeded-internal-declaration -Wno-format)
endif()

# Add Libraries with their own build setup
add_subdirectory(tpch-dbgen)
add_subdirectory(googletest)

# Add PGASUS
if(${NUMA_FOUND})
    set(PGASUS_WITH_TASKING OFF CACHE BOOL "" FORCE)
    set(PGASUS_REPLACE_MALLOC OFF CACHE BOOL "" FORCE)
    set(PGASUS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(PGASUS_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
    add_subdirectory(pgasus)
endif()

# TODO(lawben): There is currently a PR open for CMake support in libpqxx.
# Once that is merged, this should be updated to add_subdirectory(libpqxx)
add_library(
    pqxx

    STATIC

    libpqxx/src/array.cxx
    libpqxx/src/binarystring.cxx
    libpqxx/src/connection.cxx
    libpqxx/src/connection_base.cxx
    libpqxx/src/cursor.cxx
    libpqxx/src/dbtransaction.cxx
    libpqxx/src/errorhandler.cxx
    libpqxx/src/except.cxx
    libpqxx/src/field.cxx
    libpqxx/src/largeobject.cxx
    libpqxx/src/nontransaction.cxx
    libpqxx/src/notification.cxx
    libpqxx/src/pipeline.cxx
    libpqxx/src/prepared_statement.cxx
    libpqxx/src/result.cxx
    libpqxx/src/robusttransaction.cxx
    libpqxx/src/row.cxx
    libpqxx/src/statement_parameters.cxx
    libpqxx/src/strconv.cxx
    libpqxx/src/subtransaction.cxx
    libpqxx/src/tablereader.cxx
    libpqxx/src/tablestream.cxx
    libpqxx/src/tablewriter.cxx
    libpqxx/src/transaction.cxx
    libpqxx/src/transaction_base.cxx
    libpqxx/src/util.cxx
    libpqxx/src/version.cxx
)

target_compile_options(
    pqxx
    PRIVATE
    -O3 -std=c++1z
    -DPQXX_HIDE_EXP_OPTIONAL
)

target_include_directories(
    pqxx
    PUBLIC
    ${PQ_INCLUDE_DIR}
    libpqxx/include
    libpqxx/config/sample-headers/compiler/gcc-7.2
)

target_link_libraries(pqxx PUBLIC ${PQ_LIBRARY})

add_library(uninitialized_vector INTERFACE)
target_include_directories(uninitialized_vector INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/uninitialized_vector/)
target_sources(uninitialized_vector INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/uninitialized_vector/uninitialized_vector.hpp)