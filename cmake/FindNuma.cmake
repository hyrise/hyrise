# Find the numa policy library.
# Output variables:
#  NUMA_INCLUDE_DIR : e.g., /usr/include/.
#  NUMA_LIBRARY     : Library path of numa library
#  NUMA_FOUND       : True if found.

add_library(numa INTERFACE)

find_path(NUMA_INCLUDE_DIR NAME numa.h
    HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include)

find_library(NUMA_LIBRARY NAME numa
    HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
    )

if (NUMA_INCLUDE_DIR AND NUMA_LIBRARY)
    set(NUMA_FOUND TRUE)
    target_include_directories(numa INTERFACE ${NUMA_INCLUDE_DIR})
    target_link_libraries(numa INTERFACE ${NUMA_LIBRARY})
    message(STATUS "Found numa library: inc=${NUMA_INCLUDE_DIR}, lib=${NUMA_LIBRARY}")
else ()
    set(NUMA_FOUND FALSE)
    message(STATUS "WARNING: Numa library not found.")
    message(STATUS "Try: 'sudo yum install numactl numactl-devel' (or sudo apt-get install libnuma libnuma-dev)")
endif ()
