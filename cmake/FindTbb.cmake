# Find the Intel Thread Building Blocks library.
# Output variables:
#  TBB_INCLUDE_DIR : e.g., /usr/include/.
#  TBB_LIBRARY     : Library path of Intel Threading Building Blocks library
#  TBB_FOUND       : True if found.

add_library(tbb INTERFACE)

find_path(TBB_INCLUDE_DIR NAME tbb/tbb.h
    HINTS ${DEFAULT_LIB_DIRS}
    PATH_SUFFIXES include
)

find_library(TBB_LIBRARY NAME tbb
    HINTS ${DEFAULT_LIB_DIRS}
    PATH_SUFFIXES lib lib64
)

if (TBB_INCLUDE_DIR AND TBB_LIBRARY)
    set(TBB_FOUND TRUE)
    target_include_directories(tbb INTERFACE ${TBB_INCLUDE_DIR})
    target_link_libraries(tbb INTERFACE ${TBB_LIBRARY})
    message(STATUS "Found tbb library: inc=${TBB_INCLUDE_DIR}, lib=${TBB_LIBRARY}")
else ()
    set(TBB_FOUND FALSE)
    message(STATUS "WARNING: tbb library not found.")
    message(STATUS "Try: 'sudo apt-get install libtbb-dev'")
endif ()
