# Disable some warnings for the third-party subfolder
add_compile_options(-Wno-unused-function -Wno-deprecated-declarations)
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-Wno-format-overflow)
endif()

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

target_compile_options(cqf PRIVATE -Wno-format)

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

## Build lz4
set(LZ4_LIBRARY_DIR lz4/lib)

add_library (lz4
    ${LZ4_LIBRARY_DIR}/lz4.c
    ${LZ4_LIBRARY_DIR}/lz4hc.c

    ${LZ4_LIBRARY_DIR}/lz4.h
    ${LZ4_LIBRARY_DIR}/lz4hc.h
)

target_include_directories(
    lz4

    PUBLIC
    ${LZ4_LIBRARY_DIR}
)

## Build zstd
set(ZSTD_LIBRARY_DIR zstd/lib)

add_library (zstd
    ${ZSTD_LIBRARY_DIR}/compress/zstd_lazy.c
    ${ZSTD_LIBRARY_DIR}/compress/zstdmt_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_double_fast.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_fast.c
    ${ZSTD_LIBRARY_DIR}/compress/hist.c
    ${ZSTD_LIBRARY_DIR}/compress/fse_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_opt.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/huf_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_ldm.c
    ${ZSTD_LIBRARY_DIR}/common/xxhash.c
    ${ZSTD_LIBRARY_DIR}/common/fse_decompress.c
    ${ZSTD_LIBRARY_DIR}/common/pool.c
    ${ZSTD_LIBRARY_DIR}/common/zstd_common.c
    ${ZSTD_LIBRARY_DIR}/common/error_private.c
    ${ZSTD_LIBRARY_DIR}/common/debug.c
    ${ZSTD_LIBRARY_DIR}/common/threading.c
    ${ZSTD_LIBRARY_DIR}/common/entropy_common.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/zdict.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/divsufsort.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/cover.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/fastcover.c
)

target_include_directories(
    zstd

    PRIVATE
    ${ZSTD_LIBRARY_DIR}
    ${ZSTD_LIBRARY_DIR}/common
    ${ZSTD_LIBRARY_DIR}/compress
    ${ZSTD_LIBRARY_DIR}/dictBuilder
)

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

if (APPLE)
    set(JEMALLOC_LIB_PATH ${CMAKE_CURRENT_BINARY_DIR}/jemalloc/lib/libjemalloc.dylib)
else()
    set(JEMALLOC_LIB_PATH ${CMAKE_CURRENT_BINARY_DIR}/jemalloc/lib/libjemalloc.so)
endif()

include(ExternalProject)
if(APPLE)
    set(MAC_INCLUDES "SDKROOT=\"/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk\" CPATH=\"/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include\"")
else()
    set(MAC_INCLUDES "")
endif()
set(JEMALLOC_COMPILER_ENVIRONMENT "CC=\"${CMAKE_C_COMPILER_LAUNCHER} ${CMAKE_C_COMPILER}\" CXX=\"${CMAKE_CXX_COMPILER_LAUNCHER} ${CMAKE_CXX_COMPILER}\" ${MAC_INCLUDES}")
externalproject_add(
    libjemalloc

    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/jemalloc
    # Executing autoconf and the configure script with suppressed output, only printing it in case of non-zero return
    CONFIGURE_COMMAND bash -c "cd <SOURCE_DIR> && if [ ! -f ./configure ] $<SEMICOLON> then autoconf $<SEMICOLON> fi && \
                               cd <BINARY_DIR> && output=$(${JEMALLOC_COMPILER_ENVIRONMENT} <SOURCE_DIR>/configure --prefix=<BINARY_DIR> 2>&1) || \
                               (printf \"$output\\n\" && false)"
    BUILD_COMMAND bash -c "output=$(${MAC_INCLUDES} make 2>&1) || (printf \"$output\\n\" && false)"
    INSTALL_COMMAND false  # install should never be called, this is a safe guard that fails if it is
    STEP_TARGETS build
    BUILD_BYPRODUCTS ${JEMALLOC_LIB_PATH}
)
add_library(custom_jemalloc SHARED IMPORTED GLOBAL)
add_dependencies(custom_jemalloc libjemalloc-build)  # ...-build is generated by STEP_TARGETS
set_target_properties(custom_jemalloc PROPERTIES IMPORTED_LOCATION ${JEMALLOC_LIB_PATH})

# build tpcds_dbgen library
set(TPCDS_KIT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tpcds-kit/tools)
add_library(tpcds_dbgen
  STATIC
  ${TPCDS_KIT_DIR}/address.c
  ${TPCDS_KIT_DIR}/build_support.c
  ${TPCDS_KIT_DIR}/date.c
  ${TPCDS_KIT_DIR}/decimal.c
  ${TPCDS_KIT_DIR}/dist.c
  ${TPCDS_KIT_DIR}/modified_driver.c
  ${TPCDS_KIT_DIR}/error_msg.c
  ${TPCDS_KIT_DIR}/genrand.c
  ${TPCDS_KIT_DIR}/join.c
  ${TPCDS_KIT_DIR}/list.c
  ${TPCDS_KIT_DIR}/load.c
  ${TPCDS_KIT_DIR}/misc.c
  ${TPCDS_KIT_DIR}/nulls.c
  ${TPCDS_KIT_DIR}/parallel.c
  ${TPCDS_KIT_DIR}/permute.c
  ${TPCDS_KIT_DIR}/pricing.c
  ${TPCDS_KIT_DIR}/print.c
  ${TPCDS_KIT_DIR}/r_params.c
  ${TPCDS_KIT_DIR}/StringBuffer.c
  ${TPCDS_KIT_DIR}/tdef_functions.c
  ${TPCDS_KIT_DIR}/tdefs.c
  ${TPCDS_KIT_DIR}/text.c
  ${TPCDS_KIT_DIR}/scd.c
  ${TPCDS_KIT_DIR}/scaling.c
  ${TPCDS_KIT_DIR}/release.c
  ${TPCDS_KIT_DIR}/sparse.c
  ${TPCDS_KIT_DIR}/validate.c

  ${TPCDS_KIT_DIR}/dbgen_version.c

  ${TPCDS_KIT_DIR}/s_brand.c
  ${TPCDS_KIT_DIR}/s_call_center.c
  ${TPCDS_KIT_DIR}/s_catalog.c
  ${TPCDS_KIT_DIR}/s_catalog_order.c
  ${TPCDS_KIT_DIR}/s_catalog_order_lineitem.c
  ${TPCDS_KIT_DIR}/s_catalog_page.c
  ${TPCDS_KIT_DIR}/s_catalog_promotional_item.c
  ${TPCDS_KIT_DIR}/s_catalog_returns.c
  ${TPCDS_KIT_DIR}/s_category.c
  ${TPCDS_KIT_DIR}/s_class.c
  ${TPCDS_KIT_DIR}/s_company.c
  ${TPCDS_KIT_DIR}/s_customer_address.c
  ${TPCDS_KIT_DIR}/s_customer.c
  ${TPCDS_KIT_DIR}/s_division.c
  ${TPCDS_KIT_DIR}/s_inventory.c
  ${TPCDS_KIT_DIR}/s_item.c
  ${TPCDS_KIT_DIR}/s_manager.c
  ${TPCDS_KIT_DIR}/s_manufacturer.c
  ${TPCDS_KIT_DIR}/s_market.c
  ${TPCDS_KIT_DIR}/s_pline.c
  ${TPCDS_KIT_DIR}/s_product.c
  ${TPCDS_KIT_DIR}/s_promotion.c
  ${TPCDS_KIT_DIR}/s_purchase.c
  ${TPCDS_KIT_DIR}/s_reason.c
  ${TPCDS_KIT_DIR}/s_store.c
  ${TPCDS_KIT_DIR}/s_store_promotional_item.c
  ${TPCDS_KIT_DIR}/s_store_returns.c
  ${TPCDS_KIT_DIR}/s_subcategory.c
  ${TPCDS_KIT_DIR}/s_subclass.c
  ${TPCDS_KIT_DIR}/s_warehouse.c
  ${TPCDS_KIT_DIR}/s_web_order.c
  ${TPCDS_KIT_DIR}/s_web_order_lineitem.c
  ${TPCDS_KIT_DIR}/s_web_page.c
  ${TPCDS_KIT_DIR}/s_web_promotinal_item.c
  ${TPCDS_KIT_DIR}/s_web_returns.c
  ${TPCDS_KIT_DIR}/s_web_site.c
  ${TPCDS_KIT_DIR}/s_zip_to_gmt.c

  ${TPCDS_KIT_DIR}/w_call_center.c
  ${TPCDS_KIT_DIR}/w_catalog_page.c
  ${TPCDS_KIT_DIR}/w_catalog_returns.c
  ${TPCDS_KIT_DIR}/w_catalog_sales.c
  ${TPCDS_KIT_DIR}/w_customer_address.c
  ${TPCDS_KIT_DIR}/w_customer.c
  ${TPCDS_KIT_DIR}/w_customer_demographics.c
  ${TPCDS_KIT_DIR}/w_datetbl.c
  ${TPCDS_KIT_DIR}/w_household_demographics.c
  ${TPCDS_KIT_DIR}/w_income_band.c
  ${TPCDS_KIT_DIR}/w_inventory.c
  ${TPCDS_KIT_DIR}/w_item.c
  ${TPCDS_KIT_DIR}/w_promotion.c
  ${TPCDS_KIT_DIR}/w_reason.c
  ${TPCDS_KIT_DIR}/w_ship_mode.c
  ${TPCDS_KIT_DIR}/w_store.c
  ${TPCDS_KIT_DIR}/w_store_returns.c
  ${TPCDS_KIT_DIR}/w_store_sales.c
  ${TPCDS_KIT_DIR}/w_timetbl.c
  ${TPCDS_KIT_DIR}/w_warehouse.c
  ${TPCDS_KIT_DIR}/w_web_page.c
  ${TPCDS_KIT_DIR}/w_web_returns.c
  ${TPCDS_KIT_DIR}/w_web_sales.c
  ${TPCDS_KIT_DIR}/w_web_site.c)

target_include_directories(tpcds_dbgen
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR})

target_compile_definitions(
  tpcds_dbgen
  PUBLIC
  -DLINUX)
