# Find the numa policy library.
# Output variables:
#  NUMA_INCLUDE_DIR : e.g., /usr/include/.
#  NUMA_LIBRARY     : Library path of numa library
#  NUMA_FOUND       : True if found.

add_library(numa INTERFACE)

find_path(NUMA_INCLUDE_DIR NAME numa.h
    HINTS $ENV{HOME}/local /opt/local /usr/local /usr
    PATH_SUFFIXES include
)

find_library(NUMA_LIBRARY NAME numa
    HINTS $ENV{HOME}/local /usr/local /opt/local /usr
    PATH_SUFFIXES lib lib64
)

if (NUMA_INCLUDE_DIR AND NUMA_LIBRARY)
    set(NUMA_FOUND TRUE)
    target_include_directories(numa INTERFACE ${NUMA_INCLUDE_DIR})
    target_link_libraries(numa INTERFACE ${NUMA_LIBRARY})
    message(STATUS "Found numa library: inc=${NUMA_INCLUDE_DIR}, lib=${NUMA_LIBRARY}")
else ()
    set(NUMA_FOUND FALSE)
    message(STATUS "WARNING: Numa library not found.")
    message(STATUS "Try: 'sudo apt-get install libnuma libnuma-dev'")
endif ()
