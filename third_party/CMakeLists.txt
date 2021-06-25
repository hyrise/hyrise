# Disable some warnings for the third-party subfolder
add_compile_options(-Wno-unused-function -Wno-deprecated-declarations)
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-Wno-format-overflow -Wno-stringop-truncation -Wno-stringop-overflow)
endif()

# Multiple third-party libraries are not written with unity builds in minds, so we disable
# unity builds for this folder, even if the user manually activated it.
set(CMAKE_UNITY_BUILD "OFF")

# Build google benchmark
add_library(
    benchmark

    STATIC

    benchmark_fix/dummy.cc
    benchmark/src/benchmark.cc
    benchmark/src/benchmark_api_internal.cc
    benchmark/src/benchmark_name.cc
    benchmark/src/benchmark_register.cc
    benchmark/src/benchmark_runner.cc
    benchmark/src/colorprint.cc
    benchmark/src/commandlineflags.cc
    benchmark/src/complexity.cc
    benchmark/src/console_reporter.cc
    benchmark/src/counter.cc
    benchmark/src/csv_reporter.cc
    benchmark/src/json_reporter.cc
    benchmark/src/perf_counters.cc
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
    -fPIC
)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    target_compile_options(sqlparser PRIVATE -Wno-unneeded-internal-declaration -Wno-format)
endif()

# Add Libraries with their own build setup
add_subdirectory(googletest)
add_subdirectory(libpqxx)
add_subdirectory(tpch-dbgen)
add_subdirectory(nlohmann_json)
add_subdirectory(robin-map)

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

target_compile_options(lz4 PRIVATE -fPIC)

## Build zstd
set(ZSTD_LIBRARY_DIR zstd/lib)

add_library (zstd
    ${ZSTD_LIBRARY_DIR}/common/bitstream.h
    ${ZSTD_LIBRARY_DIR}/common/compiler.h
    ${ZSTD_LIBRARY_DIR}/common/cpu.h
    ${ZSTD_LIBRARY_DIR}/common/debug.c
    ${ZSTD_LIBRARY_DIR}/common/debug.h
    ${ZSTD_LIBRARY_DIR}/common/entropy_common.c
    ${ZSTD_LIBRARY_DIR}/common/error_private.c
    ${ZSTD_LIBRARY_DIR}/common/error_private.h
    ${ZSTD_LIBRARY_DIR}/common/fse.h
    ${ZSTD_LIBRARY_DIR}/common/fse_decompress.c
    ${ZSTD_LIBRARY_DIR}/common/huf.h
    ${ZSTD_LIBRARY_DIR}/common/mem.h
    ${ZSTD_LIBRARY_DIR}/common/pool.c
    ${ZSTD_LIBRARY_DIR}/common/pool.h
    ${ZSTD_LIBRARY_DIR}/common/threading.c
    ${ZSTD_LIBRARY_DIR}/common/threading.h
    ${ZSTD_LIBRARY_DIR}/common/xxhash.c
    ${ZSTD_LIBRARY_DIR}/common/xxhash.h
    ${ZSTD_LIBRARY_DIR}/common/zstd_common.c
    ${ZSTD_LIBRARY_DIR}/common/zstd_errors.h
    ${ZSTD_LIBRARY_DIR}/common/zstd_internal.h
    ${ZSTD_LIBRARY_DIR}/compress/fse_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/hist.c
    ${ZSTD_LIBRARY_DIR}/compress/hist.h
    ${ZSTD_LIBRARY_DIR}/compress/huf_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress_internal.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress_literals.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress_literals.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress_sequences.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress_sequences.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress_superblock.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_compress_superblock.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_cwksp.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_double_fast.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_double_fast.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_fast.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_fast.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_lazy.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_lazy.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_ldm.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_ldm.h
    ${ZSTD_LIBRARY_DIR}/compress/zstd_opt.c
    ${ZSTD_LIBRARY_DIR}/compress/zstd_opt.h
    ${ZSTD_LIBRARY_DIR}/compress/zstdmt_compress.c
    ${ZSTD_LIBRARY_DIR}/compress/zstdmt_compress.h
    ${ZSTD_LIBRARY_DIR}/decompress/huf_decompress.c
    ${ZSTD_LIBRARY_DIR}/decompress/zstd_ddict.c
    ${ZSTD_LIBRARY_DIR}/decompress/zstd_ddict.h
    ${ZSTD_LIBRARY_DIR}/decompress/zstd_decompress.c
    ${ZSTD_LIBRARY_DIR}/decompress/zstd_decompress_block.c
    ${ZSTD_LIBRARY_DIR}/decompress/zstd_decompress_block.h
    ${ZSTD_LIBRARY_DIR}/decompress/zstd_decompress_internal.h
    ${ZSTD_LIBRARY_DIR}/dictBuilder/cover.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/cover.h
    ${ZSTD_LIBRARY_DIR}/dictBuilder/divsufsort.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/divsufsort.h
    ${ZSTD_LIBRARY_DIR}/dictBuilder/fastcover.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/zdict.c
    ${ZSTD_LIBRARY_DIR}/dictBuilder/zdict.h
    ${ZSTD_LIBRARY_DIR}/zstd.h
)

target_include_directories(
    zstd

    PRIVATE
    ${ZSTD_LIBRARY_DIR}
    ${ZSTD_LIBRARY_DIR}/common
    ${ZSTD_LIBRARY_DIR}/compress
    ${ZSTD_LIBRARY_DIR}/dictBuilder
)

target_compile_options(zstd PRIVATE -fPIC)

add_library(compact_vector INTERFACE)
target_include_directories(compact_vector INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/compact_vector/include)
target_sources(compact_vector
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/compact_vector/include/compact_iterator.hpp
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/compact_vector/include/compact_vector.hpp
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/compact_vector/include/const_iterator_traits.hpp
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/compact_vector/include/parallel_iterator_traits.hpp
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/compact_vector/include/prefetch_iterator_traits.hpp
)
target_compile_options(compact_vector INTERFACE -Wno-cast-qual -Wno-old-style-cast -Wno-shorten-64-to-32 -Wno-shadow -Wno-reserved-id-macro -Wno-atomic-implicit-seq-cst)

add_library(uninitialized_vector INTERFACE)
target_include_directories(uninitialized_vector INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/uninitialized_vector/)
target_sources(uninitialized_vector INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/uninitialized_vector/uninitialized_vector.hpp)

if(NOT APPLE)
  # Only configure jemalloc for non-Apple builds, as we cannot properly replace the allocator on Mac using static linking:
  #   If you ./configure jemalloc with --with-jemalloc-prefix="" on OSX and static link the library, your malloc's will
  #   be handled by jemalloc. However, dynamically linked libraries will still use the default system zone. That is, any
  #   *.so files that get dynamically linked to your application will not use jemalloc, even if the main object file has
  #   been statically linked to jemalloc. This is different from linux. On linux, if the main object file is statically
  #   linked to jemalloc, then all malloc's performed by libraries dynamically linked to your application will go
  #   through jemalloc.
  #  https://github.com/exabytes18/jemalloc-example
  set(JEMALLOC_LIB_PATH ${CMAKE_CURRENT_BINARY_DIR}/jemalloc/lib/libjemalloc.so)

  if(CMAKE_CURRENT_SOURCE_DIR MATCHES "[ \t\r\n]" OR CMAKE_CURRENT_BINARY_DIR MATCHES "[ \t\r\n]")
    # It looks like `--prefix=<...>` does not work too well if the prefix has a whitespace. We did not spend too long
    # trying to get this to work as (1) this is easy to fix by the user, (2) it has only come up once in a long time, and
    # (3) Makefiles are notoriously bad with spaces: http://savannah.gnu.org/bugs/?712
    message(FATAL_ERROR "jemalloc cannot be built with whitespaces in the directory path. Please make sure Hyrise is being compiled with a path not containing any whitespaces.")
  endif()

  include(ExternalProject)
  set(JEMALLOC_COMPILER_ENVIRONMENT "CC=\"${CMAKE_C_COMPILER_LAUNCHER} ${CMAKE_C_COMPILER}\" CXX=\"${CMAKE_CXX_COMPILER_LAUNCHER} ${CMAKE_CXX_COMPILER}\"")
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

  file(GLOB_RECURSE JEMALLOC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc/*)
  externalproject_add_step(
    libjemalloc
    check_for_changes
    # Make sure that we rebuild jemalloc when a file changes
    DEPENDERS configure
    DEPENDS "${JEMALLOC_FILES}"
    COMMAND bash -c "(cd ${CMAKE_CURRENT_BINARY_DIR}/jemalloc && make distclean >/dev/null 2>/dev/null) || true"
  )
  add_library(custom_jemalloc SHARED IMPORTED GLOBAL)
  add_dependencies(custom_jemalloc libjemalloc-build)  # ...-build is generated by STEP_TARGETS
  set_target_properties(custom_jemalloc PROPERTIES IMPORTED_LOCATION ${JEMALLOC_LIB_PATH})
endif()

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

target_compile_options(
  tpcds_dbgen
  PRIVATE

  -Wno-format
  -Wno-implicit-int
  -Wno-incompatible-pointer-types
  -Wno-unused-result
)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
target_compile_options(
  tpcds_dbgen
  PRIVATE

  -Wno-parentheses-equality
)
endif()

include(ExternalProject)
externalproject_add(
    jcchDbgen

    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/jcch-dbgen
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/jcch-dbgen
    CONFIGURE_COMMAND bash -c "cp -r ${CMAKE_CURRENT_SOURCE_DIR}/jcch-dbgen/* ."
    BUILD_COMMAND bash -c "output=$(make 2>&1) || (printf \"$output\\n\" && false)"
    INSTALL_COMMAND false  # install should never be called, this is a safe guard that fails if it is
    STEP_TARGETS build
)

file(GLOB_RECURSE JCCH_DBGEN_FILES ${CMAKE_CURRENT_SOURCE_DIR}/jcch-dbgen/*)
externalproject_add_step(
  jcchDbgen
  check_for_changes
  # Make sure that we rebuild jcch-dbgen when a file changes
  DEPENDERS configure
  DEPENDS "${JCCH_DBGEN_FILES}"
  COMMAND bash -c "(cd ${CMAKE_CURRENT_BINARY_DIR}/jcch-dbgen && make distclean >/dev/null 2>/dev/null) || true"
)