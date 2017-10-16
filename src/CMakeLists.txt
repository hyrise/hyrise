if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-DIS_DEBUG=1)
else()
    add_definitions(-DIS_DEBUG=0)
endif()

if (${NUMA_FOUND})
    add_definitions(-DOPOSSUM_NUMA_SUPPORT=1)
else()
    add_definitions(-DOPOSSUM_NUMA_SUPPORT=0)
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
add_compile_options(-std=c++1z -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter)

include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/benchmark/include
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
    ${PROJECT_SOURCE_DIR}/third_party/json
    ${PROJECT_SOURCE_DIR}/third_party/sql-parser/src

    ${PROJECT_SOURCE_DIR}/src/auxlib/
    ${PROJECT_SOURCE_DIR}/src/lib/

    ${TBB_INCLUDE_DIR}
)

if (${NUMA_FOUND})
    include_directories(${PROJECT_SOURCE_DIR}/third_party/pgasus/include)
    include_directories(${PROJECT_BINARY_DIR}/third_party/pgasus/src)
endif()

add_subdirectory(auxlib)
add_subdirectory(benchmark)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(test)
