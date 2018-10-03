if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-DIS_DEBUG=1)
else()
    add_definitions(-DIS_DEBUG=0)
endif()

# Provide ENABLE_JIT_SUPPORT option and automatically disable JIT if compiler is not Clang
option(ENABLE_JIT_SUPPORT "Build with JIT support" ON)
if (NOT ${LLVM_FOUND} OR NOT "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
    set(ENABLE_JIT_SUPPORT OFF)
endif()

if (${ENABLE_JIT_SUPPORT})
    add_definitions(-DHYRISE_JIT_SUPPORT=1)
    MESSAGE(STATUS "Building with JIT support")
else()
    add_definitions(-DHYRISE_JIT_SUPPORT=0)
    MESSAGE(STATUS "Building without JIT support")
endif()

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

# Enable coverage if requested   - this is only operating on Hyrise's source (src/) so we don't check coverage of
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
# When generating a dtrace header file, symbols containing dollar-signs are created This file needs to be compiled as well.
# Hence, the `-Wno-dollar-in-identifier-extension` flag is required.
add_compile_options(-std=c++17 -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension)

include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/benchmark/include
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
    ${PROJECT_SOURCE_DIR}/third_party/cxxopts/include
    ${PROJECT_SOURCE_DIR}/third_party/json
    ${PROJECT_SOURCE_DIR}/third_party/sql-parser/src
    ${PROJECT_SOURCE_DIR}/third_party/cpp-btree

    ${PROJECT_SOURCE_DIR}/src/benchmarklib/
    ${PROJECT_SOURCE_DIR}/src/lib/
    ${PROJECT_SOURCE_DIR}/src/plugin/

    ${TBB_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS}
)

if (${ENABLE_JIT_SUPPORT})
    include_directories(${LLVM_INCLUDE_DIR})
endif()

if (${ENABLE_NUMA_SUPPORT})
    include_directories(${PROJECT_SOURCE_DIR}/third_party/pgasus/include)
    include_directories(${PROJECT_BINARY_DIR}/third_party/pgasus/src)
endif()

set(ENABLE_CLANG_TIDY OFF CACHE BOOL "Run clang-tidy")
if (ENABLE_CLANG_TIDY)
    message(STATUS "clang-tidy enabled")
    set(CMAKE_CXX_CLANG_TIDY "${CMAKE_SOURCE_DIR}/scripts/clang_tidy_wrapper.sh;${CMAKE_SOURCE_DIR}")

    set_source_files_properties(${CMAKE_SOURCE_DIR}/src/lib/operators/jit_operator/specialization/llvm/CloneFunction.cpp ${CMAKE_SOURCE_DIR}/src/lib/operators/jit_operator/specialization/llvm/InlineFunction.cpp PROPERTIES CMAKE_CXX_CLANG_TIDY "")
endif()

add_subdirectory(benchmarklib)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(plugins)

# No clang-tidy for the following subfolders
set(CMAKE_CXX_CLANG_TIDY "")
add_subdirectory(benchmark)
add_subdirectory(test)