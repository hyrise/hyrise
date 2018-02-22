if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-DIS_DEBUG=1)
else()
    add_definitions(-DIS_DEBUG=0)
endif()

option(DISABLE_JIT_SUPPORT "Build without JIT support" OFF)
if (NOT ${LLVM_FOUND} OR NOT "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
    set(DISABLE_JIT_SUPPORT ON)
endif()

if (NOT DISABLE_JIT_SUPPORT)
    add_definitions(-DHYRISE_JIT_SUPPORT=1)
else()
    add_definitions(-DHYRISE_JIT_SUPPORT=0)
    MESSAGE(STATUS "Building without JIT support")
endif()

option(DISABLE_NUMA_SUPPORT "Build without NUMA support" OFF)
if (NOT DISABLE_NUMA_SUPPORT AND ${NUMA_FOUND})
    add_definitions(-DHYRISE_NUMA_SUPPORT=1)
else()
    add_definitions(-DHYRISE_NUMA_SUPPORT=0)
    MESSAGE(STATUS "Building without NUMA support")
endif()

# Enable coverage if requested   - this is only operating on Hyrise's source (src/) so we don't check coverage of
# third_party stuff
option(ENABLE_COVERAGE "Set to ON to build Hyrise with enabled coverage checking. Default: OFF" OFF)
if (${ENABLE_COVERAGE})
    add_compile_options(-O0 -fprofile-arcs -ftest-coverage)
    set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} --coverage")
endif()

# This will be used by the DebugAssert macro to output
# a file path relative to CMAKE_SOURCE_DIR
string(LENGTH "${CMAKE_SOURCE_DIR}/" SOURCE_PATH_SIZE)
if (CMAKE_CXX_COMPILER_LAUNCHER)
    if (${CMAKE_CXX_COMPILER_LAUNCHER} STREQUAL "ccache")
        string(LENGTH "" SOURCE_PATH_SIZE)
    endif()
endif()
add_definitions("-DSOURCE_PATH_SIZE=${SOURCE_PATH_SIZE}")

# Global flags and include directories
add_compile_options(-std=c++17 -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter)

include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/benchmark/include
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
    ${PROJECT_SOURCE_DIR}/third_party/cxxopts/include
    ${PROJECT_SOURCE_DIR}/third_party/json
    ${PROJECT_SOURCE_DIR}/third_party/sql-parser/src

    ${PROJECT_SOURCE_DIR}/src/benchmarklib/
    ${PROJECT_SOURCE_DIR}/src/lib/

    ${TBB_INCLUDE_DIR}
)

if (NOT DISABLE_JIT_SUPPORT)
    include_directories(${LLVM_INCLUDE_DIR})
endif()

if (NOT DISABLE_NUMA_SUPPORT AND ${NUMA_FOUND})
    include_directories(${PROJECT_SOURCE_DIR}/third_party/pgasus/include)
    include_directories(${PROJECT_BINARY_DIR}/third_party/pgasus/src)
endif()

add_subdirectory(benchmarklib)
add_subdirectory(benchmark)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(test)
