# consider updating DEPENDENCIES.md when you touch this line
cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(Hyrise)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.2)
        message(FATAL_ERROR "Your GCC version ${CMAKE_CXX_COMPILER_VERSION} is too old.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.0)
        message(FATAL_ERROR "Your clang version ${CMAKE_CXX_COMPILER_VERSION} is too old.")
    endif()
else()
    message(WARNING "You are using an unsupported compiler (${CMAKE_CXX_COMPILER_ID})! Compilation has only been tested with Clang and GCC.")
endif()

# Enable sanitization if requested
option(ENABLE_SANITIZATION "Set to ON to build Hyrise with ASAN and UBSAN enabled. Default: OFF" OFF)
if (${ENABLE_SANITIZATION})
    # add_compile_options() wants list, CMAKE_EXE_LINKER_FLAGS a string. There are probably cleverer ways than
    # duplicating the flags, but this is prbly the simplest solution.
    add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
    set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer")
endif()


# Set default build type if none was passed on the command line
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# CMake settings
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH}) # To allow CMake to locate our Find*.cmake files
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}) # Put binaries into root of build tree
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib) # Put libraries into their own dir

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -march=native")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")

# Link Time Optimization (LTO)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    if(NOT ${CMAKE_VERSION} VERSION_LESS "3.9.0")
        cmake_minimum_required(VERSION 3.9 FATAL_ERROR)
        include(CheckIPOSupported)
        check_ipo_supported()
        set_target_properties(PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
        add_definitions(-DWITH_LTO)
    endif()
endif()

# Require NCurses over Curses
set(CURSES_NEED_NCURSES TRUE)

# Dependencies
find_package(Numa)
find_package(Tbb REQUIRED)
find_package(Readline REQUIRED)
find_package(Curses REQUIRED)
find_package(Sqlite3 REQUIRED)

# Include sub-CMakeLists.txt
add_subdirectory(third_party/ EXCLUDE_FROM_ALL)
add_subdirectory(src)

# Useful for printing all c++ files in a directory:
# find . -type f -name "*.cpp" -o -name "*.hpp" | cut -c 3- | sort
