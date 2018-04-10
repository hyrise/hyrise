# Find PostgreSQL C++ library and header file
# Sets
#   PQXX_FOUND        : TRUE if found
#   PQXX_INCLUDE_DIRS : to directories containing pqxx and postgres' pq
#   PQXX_LIBRARIES    : library path of libpqxx and postgres' libpq

FIND_PACKAGE(PQ REQUIRED)
IF (PQ_FOUND)

    FIND_PATH(PQXX_INCLUDE_DIR NAME pqxx/pqxx HINTS
        "$ENV{LIB_DIR}/include"
        "$ENV{LIB_DIR}/include/pqxx"
        "$ENV{INCLUDE}"
    )

    FIND_LIBRARY(PQXX_LIBRARY NAMES libpqxx pqxx PATHS
        "$ENV{LIB_DIR}/lib"
        "$ENV{LIB}/lib"
    )

    IF(PQXX_INCLUDE_DIR AND PQXX_LIBRARY)
        SET(PQXX_FOUND TRUE)
        SET(PQXX_INCLUDE_DIRS "${PQXX_INCLUDE_DIR};${PQ_INCLUDE_DIR}")
        SET(PQXX_LIBRARIES "${PQXX_LIBRARY};${PQ_LIBRARY}")
        MESSAGE(STATUS "Found pqxx library: inc=${PQXX_INCLUDE_DIR}, lib=${PQXX_LIBRARY}")
    ELSE()
        MESSAGE(STATUS "WARING: PQXX library not found.")
        MESSAGE(STATUS "Try: 'sudo yum install libpqxx-dev' (or sudo apt-get install libpqxx-dev)")
    ENDIF()
ENDIF()
