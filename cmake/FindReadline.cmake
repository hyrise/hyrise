# Find the readline library. This is necessary on macOS to find the homebrew version
# Output variables:
#  READLINE_INCLUDE_DIR : e.g., /usr/include/.
#  READLINE_LIBRARY     : Library path of readline library
#  READLINE_FOUND       : True if found.

add_library(readline INTERFACE)

find_path(READLINE_INCLUDE_DIR NAME readline/readline.h
    HINTS /usr/local/opt/readline /opt/homebrew/opt/readline ${DEFAULT_LIB_DIRS}
    PATH_SUFFIXES include
)

find_library(READLINE_LIBRARY NAME readline
    HINTS /usr/local/opt/readline /opt/homebrew/opt/readline ${DEFAULT_LIB_DIRS}
    PATH_SUFFIXES lib lib64
)

if (READLINE_INCLUDE_DIR AND READLINE_LIBRARY)
    set(READLINE_FOUND TRUE)
    target_include_directories(readline INTERFACE ${READLINE_INCLUDE_DIR})
    target_link_libraries(readline INTERFACE ${READLINE_LIBRARY})
    message(STATUS "Found readline library: inc=${READLINE_INCLUDE_DIR}, lib=${READLINE_LIBRARY}")
else ()
    set(READLINE_FOUND FALSE)
    message(STATUS "WARNING: readline library not found.")
    message(STATUS "Try: 'sudo apt-get install libreadline-dev'")
endif ()
