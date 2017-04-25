cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(OpossumDB)

# Set default build type and set IS_DEBUG
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-DIS_DEBUG=1)
else()
    add_definitions(-DIS_DEBUG=0)
endif()

# Global flags and include directories
set(${CMAKE_CXX_FLAGS} "${CMAKE_CXX_FLAGS} -std=c++1z -pthread -Wno-error=unused-parameter -Wall -Werror")

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

# Include sub-CMakeLists.txt
add_subdirectory(src/benchmark)
add_subdirectory(src/bin)
add_subdirectory(src/lib)
add_subdirectory(src/test)
add_subdirectory(third_party/benchmark)
add_subdirectory(third_party/googletest)

# Useful for printing all c++ files in a directory ´find . -regex ".*/.*\.\(cpp\|hpp\|cc\)" -type f -printf '%P\n'´