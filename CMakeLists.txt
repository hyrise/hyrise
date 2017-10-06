cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(OpossumDB)

# Set default build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# CMake settings
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH}) # To allow CMake to locate our Find*.cmake files
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}) # Put binaries into root of build tree
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib) # Put libraries into their own dir
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")

# Require NCurses over Curses
set(CURSES_NEED_NCURSES TRUE)

# Dependencies
find_package(Numa)
find_package(Tbb REQUIRED)
find_package(Readline REQUIRED)
find_package(Curses REQUIRED)

# Include sub-CMakeLists.txt
add_subdirectory(third_party/)
add_subdirectory(third_party/googletest)
add_subdirectory(src)

if(${NUMA_FOUND})
    set(PGASUS_WITH_TASKING OFF CACHE BOOL "" FORCE)
    set(PGASUS_REPLACE_MALLOC OFF CACHE BOOL "" FORCE)
    set(PGASUS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(PGASUS_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
    add_subdirectory(third_party/pgasus)
endif()

# Useful for printing all c++ files in a directory:
# find . -type f -name "*.cpp" -o -name "*.hpp" | cut -c 3- | sort
