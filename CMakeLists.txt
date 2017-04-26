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

# Dependencies
find_package(Numa)

# Include sub-CMakeLists.txt
add_subdirectory(third_party/)
add_subdirectory(src)
add_subdirectory(third_party/googletest)

# Useful for printing all c++ files in a directory ´find . -regex ".*/.*\.\(cpp\|hpp\|cc\)" -type f -printf '%P\n'´
