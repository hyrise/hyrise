# Find the C++ stdlib filesytem library.
# Output variables:
#  FILESYSTEM_LIBRARY  : Library path of filesystem library
#  FILESYSTEM_FOUND    : True if found.

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    FIND_LIBRARY(FILESYSTEM_LIBRARY NAME libc++experimental.a HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib /usr/local/Cellar/llvm/*/lib)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    FIND_LIBRARY(FILESYSTEM_LIBRARY NAME libstdc++fs.a HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib /usr/lib/gcc/x86_64-linux-gnu/*/ /usr/local/Cellar/gcc/*/lib/gcc/*/)
endif()

IF (FILESYSTEM_LIBRARY)
    SET(FILESYSTEM_FOUND TRUE)
    MESSAGE(STATUS "Found C++ stdlib filesystem library: lib=${FILESYSTEM_LIBRARY}")
ELSE ()
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND UNIX AND NOT APPLE)
        SET(FILESYSTEM_FOUND TRUE)
        SET(FILESYSTEM_LIBRARY "")
        MESSAGE(STATUS "Clang has built-in support for C++ stdlib filesystem features")
        set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -stdlib=libc++")
    ELSE ()
        SET(FILESYSTEM_FOUND FALSE)
        MESSAGE(STATUS "Error: C++ stdlib filesystem library.")
        MESSAGE(STATUS "Try installing a newer version of clang/gcc")
    ENDIF ()
ENDIF ()
