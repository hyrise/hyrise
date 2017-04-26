cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(OpossumDB)

# Set default build type and set IS_DEBUG
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# CMake settings
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-DIS_DEBUG=1)
else()
    add_definitions(-DIS_DEBUG=0)
endif()

# Dependencies
find_package(Numa)

if (${NUMA_FOUND})
    add_definitions(-DOPOSSUM_NUMA_SUPPORT=1)
else()
    add_definitions(-DOPOSSUM_NUMA_SUPPORT=0)
endif()

# Include sub-CMakeLists.txt
add_subdirectory(src)
add_subdirectory(third_party/benchmark)
add_subdirectory(third_party/googletest)

# Useful for printing all c++ files in a directory ´find . -regex ".*/.*\.\(cpp\|hpp\|cc\)" -type f -printf '%P\n'´
