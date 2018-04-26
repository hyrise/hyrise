# Find PostgreSQL C library and header file
# Sets
#   PQ_FOUND       : TRUE if found
#   PQ_INCLUDE_DIR : to directory containing postgres' pq
#   PQ_LIBRARY     : library path of postgres' pq

FIND_PATH(PQ_INCLUDE_DIR NAME libpq-fe.h HINTS
    $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include /usr/local/include/postgresql /usr/include/postgresql /usr/local/opt/libpq/include
)

FIND_LIBRARY(PQ_LIBRARY NAME pq HINTS
    "$ENV{LIB_DIR}/lib" "$ENV{LIB}/lib" "$ENV{HOME}/local/lib64" "$ENV{HOME}/local/lib" /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib /usr/local/opt/libpq/lib
)

IF(PQ_INCLUDE_DIR AND PQ_LIBRARY)
    SET(PQ_FOUND TRUE)
    MESSAGE(STATUS "Found pq library: inc=${PQ_INCLUDE_DIR}, lib=${PQ_LIBRARY}")
ELSE()
    MESSAGE(STATUS "WARING: pq library not found.")
    MESSAGE(STATUS "Try: 'sudo yum install libpq-dev' (or sudo apt-get install libpq-dev)")
ENDIF()
