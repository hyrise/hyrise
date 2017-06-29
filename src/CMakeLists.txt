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

# Global flags and include directories
add_compile_options(-std=c++1z -pthread -Wno-error=unused-parameter -Wall -Werror)

include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/benchmark/include
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
    ${PROJECT_SOURCE_DIR}/third_party/grpc/include
    ${PROJECT_SOURCE_DIR}/third_party/grpc/third_party/protobuf/src
    ${PROJECT_SOURCE_DIR}/third_party/sql-parser/src

    ${PROJECT_SOURCE_DIR}/src/lib/
)

link_directories(
    ${PROJECT_SOURCE_DIR}/third_party/grpc/libs/opt/
    ${PROJECT_SOURCE_DIR}/third_party/grpc/libs/opt/protobuf
)

add_subdirectory(benchmark)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(test)
