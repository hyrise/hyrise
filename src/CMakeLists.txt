if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(HYRISE_DEBUG 1)
else()
    set(HYRISE_DEBUG 0)
endif()

add_definitions(-DHYRISE_DEBUG=${HYRISE_DEBUG})

# Provide ENABLE_NUMA_SUPPORT option and automatically disable NUMA if libNUMA was not found
option(ENABLE_NUMA_SUPPORT "Build with NUMA support" ON)
if (NOT ${NUMA_FOUND})
    set(ENABLE_NUMA_SUPPORT OFF)
endif()

if (${ENABLE_NUMA_SUPPORT})
    add_definitions(-DHYRISE_NUMA_SUPPORT=1)
    MESSAGE(STATUS "Building with NUMA support")
else()
    add_definitions(-DHYRISE_NUMA_SUPPORT=0)
    MESSAGE(STATUS "Building without NUMA support")
endif()

# Enable coverage if requested - this is only operating on Hyrise's source (src/) so we don't check coverage of
# third_party stuff
option(ENABLE_COVERAGE "Set to ON to build Hyrise with enabled coverage checking. Default: OFF" OFF)
if (${ENABLE_COVERAGE})
    add_compile_options(-O0 -fno-inline)

    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fprofile-arcs -ftest-coverage)
        set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
        set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    else()
        message(FATAL_ERROR "Don't know how to run coverage on your compiler (${CMAKE_CXX_COMPILER_ID}).")
    endif()
endif()

# Global flags and include directories
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)

# When generating a dtrace header file, symbols containing dollar-signs are created This file needs to be compiled as well.
# Hence, the `-Wno-dollar-in-identifier-extension` flag is required.
add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    add_compile_options(-Weverything -Wshadow-all -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-documentation -Wno-padded -Wno-global-constructors -Wno-sign-conversion -Wno-exit-time-destructors -Wno-shadow-field-in-constructor -Wno-shadow-field -Wno-switch-enum -Wno-weak-vtables -Wno-double-promotion -Wno-covered-switch-default -Wno-unused-macros -Wno-newline-eof -Wno-missing-variable-declarations -Wno-weak-template-vtables -Wno-missing-prototypes -Wno-float-equal -Wno-return-std-move-in-c++11 -Wno-unreachable-code-break -Wno-undefined-func-template -Wno-unknown-warning-option -Wno-pass-failed -Wno-ctad-maybe-unsupported)
endif()

include(${PROJECT_SOURCE_DIR}/cmake/TargetLinkLibrariesSystem.cmake)

include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
    ${PROJECT_SOURCE_DIR}/third_party/sql-parser/src

    ${PROJECT_SOURCE_DIR}/src/benchmarklib/
    ${PROJECT_SOURCE_DIR}/src/lib/
    ${PROJECT_SOURCE_DIR}/src/plugin/

    ${Boost_INCLUDE_DIRS}
)

# Include these libraries as system libraries to silence some warnings caused by -Weverything
include_directories(
    SYSTEM
    ${TBB_INCLUDE_DIR}
    ${PROJECT_SOURCE_DIR}/third_party/benchmark/include
    ${PROJECT_SOURCE_DIR}/third_party/cpp-btree
    ${PROJECT_SOURCE_DIR}/third_party/cqf/include
    ${PROJECT_SOURCE_DIR}/third_party/cxxopts/include
    ${PROJECT_SOURCE_DIR}/third_party/flat_hash_map
    ${PROJECT_SOURCE_DIR}/third_party/json
    ${PROJECT_SOURCE_DIR}/third_party/lz4
    ${PROJECT_SOURCE_DIR}/third_party/zstd
    ${PROJECT_SOURCE_DIR}/third_party/tpcds-result-reproduction
)

if (${ENABLE_NUMA_SUPPORT})
    include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third_party/pgasus/include)
    include_directories(SYSTEM ${PROJECT_BINARY_DIR}/third_party/pgasus/src)
endif()

set(ENABLE_CLANG_TIDY OFF CACHE BOOL "Run clang-tidy")
if (ENABLE_CLANG_TIDY)
    message(STATUS "clang-tidy enabled")
    set(CMAKE_CXX_CLANG_TIDY "${CMAKE_SOURCE_DIR}/scripts/clang_tidy_wrapper.sh;${CMAKE_SOURCE_DIR}")
endif()

add_subdirectory(benchmarklib)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(plugins)

# No clang-tidy for the following subfolders
set(CMAKE_CXX_CLANG_TIDY "")
add_subdirectory(benchmark)
add_subdirectory(test)
