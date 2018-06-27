# Find the systemtap library.
# Output variables:
#  SYSTEMTAP_INCLUDE_DIR : e.g., /usr/include/.
#  SYSTEMTAP_FOUND       : True if found.
FIND_PATH(SYSTEMTAP_INCLUDE_DIR NAME sys/sdt.h
    HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include /usr/include/x86_64-linux-gnu/ )

IF (SYSTEMTAP_INCLUDE_DIR)
    SET(SYSTEMTAP_FOUND TRUE)
    MESSAGE(STATUS "Found systemtap library: inc=${SYSTEMTAP_INCLUDE_DIR}")
ELSE ()
    SET(SYSTEMTAP_FOUND FALSE)
    MESSAGE(STATUS "WARNING: systemtap library not found.")
    MESSAGE(STATUS "Try: 'apt install systemtap-sdt-dev' (or sudo apt install systemtap-sdt-dev)")
ENDIF ()