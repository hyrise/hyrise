# Find the sqlite3 library.
# Output variables:
#  SQLITE3_INCLUDE_DIR : e.g., /usr/include/.
#  SQLITE3_LIBRARY     : Library path of sqlite3 library
#  SQLITE3_FOUND       : True if found.

FIND_PATH(SQLITE3_INCLUDE_DIR NAME sqlite3.h HINTS
    "$ENV{LIB_DIR}/include"
    "$ENV{LIB_DIR}/include/sqlite"
    "$ENV{INCLUDE}"
)

FIND_LIBRARY(SQLITE3_LIBRARY NAMES sqlite3_i sqlite3 PATHS
  "$ENV{LIB_DIR}/lib"
  "$ENV{LIB}/lib"
  )

IF (SQLITE3_INCLUDE_DIR AND SQLITE3_LIBRARY)
    SET(SQLITE3_FOUND TRUE)
    MESSAGE(STATUS "Found sqlite3 library: inc=${SQLITE3_INCLUDE_DIR}, lib=${SQLITE3_LIBRARY}")
ELSE ()
    SET(SQLITE3_FOUND FALSE)
    MESSAGE(STATUS "WARNING: sqlite3 library not found.")
    MESSAGE(STATUS "Try: 'sudo yum install libsqlite3-dev' (or sudo apt-get install libsqlite3-dev)")
ENDIF ()
