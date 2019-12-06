# consider updating DEPENDENCIES.md when you touch this line
cmake_minimum_required(VERSION 3.9 FATAL_ERROR)

project(Hyrise)

if(APPLE)
    set(CMAKE_EXE_LINKER_FLAGS -Wl,-export_dynamic)
else()
    set(CMAKE_EXE_LINKER_FLAGS -Wl,--export-dynamic)
endif()

option(ENABLE_UNSUPPORTED_COMPILER "Set to ON to build Hyrise even if the compiler is not supported. Default: OFF" OFF)
function(compiler_not_supported message)
    if (${ENABLE_UNSUPPORTED_COMPILER})
        message(WARNING ${message})
    else()
        message(FATAL_ERROR "${message} You can ignore this error by setting -DENABLE_UNSUPPORTED_COMPILER=ON.")
    endif()
endfunction(compiler_not_supported)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.1)
        compiler_not_supported("Your GCC version ${CMAKE_CXX_COMPILER_VERSION} is too old.")
    endif()
    if (APPLE)
        # https://software.intel.com/en-us/forums/intel-threading-building-blocks/topic/749200
        compiler_not_supported("We had to drop support for GCC on OS X because it caused segfaults when used with tbb. You can continue, but don't hold us responsible for any segmentation faults.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
        compiler_not_supported("Your clang version ${CMAKE_CXX_COMPILER_VERSION} is too old.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    if(NOT EXISTS "/usr/local/opt/llvm/bin/clang")
        set(CMAKE_OSX_INSTALL_TEXT "install the official llvm/clang package using `install.sh` or `brew install llvm --with-toolchain` and then ")
    endif()
    compiler_not_supported("Apple's clang compiler is not supported because it is lacking support for recent C++ features. Please ${CMAKE_OSX_INSTALL_TEXT}run cmake with `-DCMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++`.")
else()
    compiler_not_supported("You are using an unsupported compiler (${CMAKE_CXX_COMPILER_ID})! Compilation has only been tested with Clang (Linux + OS X) and GCC (Linux).")
endif()

# Enable address and undefined behavior sanitization if requested
option(ENABLE_ADDR_UB_SANITIZATION "Set to ON to build Hyrise with ASAN and UBSAN enabled. Default: OFF" OFF)
if (${ENABLE_ADDR_UB_SANITIZATION})
    # add_compile_options() wants list, CMAKE_EXE_LINKER_FLAGS a string. There are probably cleverer ways than
    # duplicating the flags, but this is probably the simplest solution.
    add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
    set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer")
endif()

# Enable thread sanitization if requested
option(ENABLE_THREAD_SANITIZATION "Set to ON to build Hyrise with TSAN enabled. Default: OFF" OFF)
if (${ENABLE_THREAD_SANITIZATION})
    # add_compile_options() wants list, CMAKE_EXE_LINKER_FLAGS a string. There are probably cleverer ways than
    # duplicating the flags, but this is probably the simplest solution.
    add_compile_options(-fsanitize=thread -O1)
    add_link_options(-fsanitize=thread)
    set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif()

# Set default build type if none was passed on the command line
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
set(BUILD_TYPES Debug RelWithDebInfo Release)
if(NOT CMAKE_BUILD_TYPE IN_LIST BUILD_TYPES)
    message(FATAL_ERROR "Unknown Build Type: ${CMAKE_BUILD_TYPE}")
endif()

# CMake settings
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH}) # To allow CMake to locate our Find*.cmake files
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}) # Put binaries into root of build tree
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib) # Put libraries into their own dir

# C(++) Flags
set(FLAGS_ALL "-fopenmp-simd")  # enables loop vectorization hints, but does not include the OpenMP runtime
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${FLAGS_ALL}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${FLAGS_ALL}")

set(FLAGS_RELEASE "-march=native")  # build the binary optimized for the current system, ignoring older systems
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${FLAGS_ALL} ${FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${FLAGS_ALL} ${FLAGS_RELEASE}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${FLAGS_ALL} ${FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${FLAGS_ALL} ${FLAGS_RELEASE}")

# Check for unity builds
if(${CMAKE_VERSION} VERSION_LESS "3.16.0")
    if(DEFINED CMAKE_UNITY_BUILD)
        message(FATAL_ERROR "-DCMAKE_UNITY_BUILD=ON was set, but your cmake version does not support unity builds.")
    endif()
endif()

# On Linux, use the lld linker, which is twice as fast for incremental debug builds
if (NOT APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "-fuse-ld=lld")
    set(CMAKE_EXE_LINKER_FLAGS "-fuse-ld=lld")
endif()

# Require NCurses over Curses
set(CURSES_NEED_NCURSES TRUE)

# Surprisingly, this makes the linking process on Mac faster by 5x
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_SHARED_LINKER_FLAGS_DEBUG "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -O0")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -O0")

# Dependencies
find_package(Numa QUIET)
find_package(Tbb REQUIRED)
find_package(Readline REQUIRED)
find_package(Curses REQUIRED)
find_package(Sqlite3 REQUIRED)
find_package(Boost REQUIRED COMPONENTS container system)

add_definitions(-DBOOST_THREAD_VERSION=5)

# If we are building Hyrise for the CI server, we want to make sure that all optional features are available and can be tested
if(CI_BUILD)
    if(NOT ${NUMA_FOUND})
        message(FATAL_ERROR "-DCI_BUILD=ON was set, but libnuma was not found.")
    endif()
endif()

# Link Time Optimization (LTO)
cmake_policy(SET CMP0069 NEW)
option(ENABLE_LTO "Set to ON to build Hyrise with enabled Link Time Optimization (LTO). Default: OFF" OFF)
if (${ENABLE_LTO})
    if(NOT ${CMAKE_VERSION} VERSION_LESS "3.9.0")
        cmake_minimum_required(VERSION 3.9 FATAL_ERROR)
        include(CheckIPOSupported)
        check_ipo_supported(RESULT ipo_supported OUTPUT ipo_output)
        if(ipo_supported)
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
            add_definitions(-DWITH_LTO)
            message(STATUS "Building with Link-Time Optimization")
        else()
            message(WARNING "LTO not supported: ${ipo_output}")
        endif()
    endif()
endif()

# Include sub-CMakeLists.txt
add_subdirectory(third_party/ EXCLUDE_FROM_ALL)

# Some third-party libs don't support LTO (if enabled above):
foreach(no_lto_target gtest gtest_main gmock gmock_main hpinuma_s hpinuma hpinuma_base_s hpinuma_base hpinuma_util_tool hpinuma_msource hpinuma_msource_s)
    if(TARGET no_lto_target)
        set_property(TARGET ${no_lto_target} PROPERTY INTERPROCEDURAL_OPTIMIZATION FALSE)
    endif()
endforeach(no_lto_target)
add_subdirectory(src)


# Useful for printing all c++ files in a directory:
# find . -type f -name "*.cpp" -o -name "*.hpp" | cut -c 3- | sort
