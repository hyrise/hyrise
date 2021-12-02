# Find the sqlite3 library.
# Output variables:
#  SQLITE3_INCLUDE_DIR : e.g., /usr/include/.
#  SQLITE3_LIBRARY     : Library path of sqlite3 library
#  SQLITE3_FOUND       : True if found.

add_library(sqlite3 INTERFACE)

find_path(SQLITE3_INCLUDE_DIR NAME sqlite3.h HINTS
    "$ENV{LIB_DIR}/include"
    "$ENV{LIB_DIR}/include/sqlite"
    "$ENV{INCLUDE}"
)

find_library(SQLITE3_LIBRARY NAMES sqlite3_i sqlite3 PATHS
  "$ENV{LIB_DIR}/lib"
  "$ENV{LIB}/lib"
  )

if (SQLITE3_INCLUDE_DIR AND SQLITE3_LIBRARY)
    set(SQLITE3_FOUND TRUE)
    target_include_directories(sqlite3 INTERFACE ${SQLITE3_INCLUDE_DIR})
    target_link_libraries(sqlite3 INTERFACE ${SQLITE3_LIBRARY})
    message(STATUS "Found sqlite3 library: inc=${SQLITE3_INCLUDE_DIR}, lib=${SQLITE3_LIBRARY}")
else ()
    set(SQLITE3_FOUND FALSE)
    message(STATUS "WARNING: sqlite3 library not found.")
    message(STATUS "Try: 'sudo yum install libsqlite3-dev' (or sudo apt-get install libsqlite3-dev)")
endif ()
