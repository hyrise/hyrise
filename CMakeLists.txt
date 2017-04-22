cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(OpossumDB)

list(APPEND CMAKE_CXX_FLAGS -std=c++1z)

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-DIS_DEBUG=1)
else()
    add_definitions(-DIS_DEBUG=0)
endif()

include_directories(
    third_party/benchmark/include
    third_party/googletest/googletest/include
    third_party/grpc/include
    third_party/grpc/third_party/protobuf/src

    src/lib/
)

link_directories(
    third_party/grpc/libs/opt/
    third_party/grpc/libs/opt/protobuf
)

add_subdirectory(src/benchmark)
add_subdirectory(src/lib)
add_subdirectory(src/test)
add_subdirectory(third_party/benchmark)
add_subdirectory(third_party/googletest)

# Fetch with ´find . -regex ".*/.*\.\(cpp\|hpp\|cc\)" -type f -printf '%P\n'´