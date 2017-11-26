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

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    target_compile_options(sqlparser PRIVATE -Wno-unneeded-internal-declaration -Wno-format)
endif()

# Add google test
add_subdirectory(googletest)

# Add PGASUS
if(${NUMA_FOUND})
    set(PGASUS_WITH_TASKING OFF CACHE BOOL "" FORCE)
    set(PGASUS_REPLACE_MALLOC OFF CACHE BOOL "" FORCE)
    set(PGASUS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(PGASUS_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
    add_subdirectory(pgasus )
endif()