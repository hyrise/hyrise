# Find the readline library. This is necessary on macOS to find the homebrew version
# Output variables:
#  READLINE_INCLUDE_DIR : e.g., /usr/include/.
#  READLINE_LIBRARY     : Library path of readline library
#  READLINE_FOUND       : True if found.
FIND_PATH(READLINE_INCLUDE_DIR NAME readline/readline.h
    HINTS /usr/local/opt/readline/include/ $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include)

FIND_LIBRARY(READLINE_LIBRARY NAME readline
    HINTS /usr/local/opt/readline/lib/ $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
    )

IF (READLINE_INCLUDE_DIR AND READLINE_LIBRARY)
    SET(READLINE_FOUND TRUE)
    MESSAGE(STATUS "Found readline library: inc=${READLINE_INCLUDE_DIR}, lib=${READLINE_LIBRARY}")
ELSE ()
    SET(READLINE_FOUND FALSE)
    MESSAGE(STATUS "WARNING: readline library not found.")
    MESSAGE(STATUS "Try: 'sudo yum install libreadline-dev' (or sudo apt-get install libreadline-dev)")
ENDIF ()
