# This is taken from: https://github.com/andrew-hardin/cmake-git-version-tracking with slight modifications.
#
# All glory and blame should go to Andrew Hardin
#
# git_watcher.cmake
#
# This file defines the functions and targets needed to keep a
# watch on the state of the git repo. If the state changes, a
# header is reconfigured.
#
# Customization tip:
#   - You should only need to edit the paths to the pre and
#     post configure file. The rest should be plug-and-play.
#     See below for where those variables are defined.
#
# Script design:
#   - This script was designed similar to a Python application
#     with a Main() function. I wanted to keep it compact to
#     simplify "copy + paste" usage.
#
#   - This script is made to operate in two scopes:
#       1. Configure time scope (when build files are created).
#       2. Build time scope (called via CMake -P)
#     If you see something odd (e.g. the NOT DEFINED clauses),
#     consider that it can run in one of two scopes.

if(NOT DEFINED post_configure_file)
    set(post_configure_file "${CMAKE_BINARY_DIR}/version.hpp")
endif()
if(NOT DEFINED pre_configure_file)
    set(pre_configure_file "${PROJECT_SOURCE_DIR}/src/lib/version.hpp.in")
endif()
if(NOT EXISTS "${pre_configure_file}")
    message(FATAL_ERROR "Runtime error: the preconfigure file doesn't exist.")
endif()


# This variable describes where we record the state of the git repo.
set(git_state_file "${CMAKE_CURRENT_BINARY_DIR}/git-state")


# Function: GitStateChangedAction
# Description: this action is executed when the state of the git
#              repo changes (e.g. a commit is made).
function(GitStateChangedAction)
    # Read the git state file
    file(STRINGS "${git_state_file}" CONTENT)
    LIST(GET CONTENT 0 HELP_STRING)
    LIST(GET CONTENT 1 GIT_RETRIEVED_STATE)
    LIST(GET CONTENT 2 GIT_HEAD_SHA1)
    LIST(GET CONTENT 3 GIT_IS_DIRTY)
    # Configure the file.
    configure_file("${pre_configure_file}" "${post_configure_file}" @ONLY)
endfunction()


###################################################
# There be dragons below here...                  #
###################################################


# Function: GetGitState
# Description: gets the current state of the git repo.
# Args:
#   _working_dir (in)  string; the directory from which git commands will be ran.
#   _hashvar     (out) string; the SHA1 hash for HEAD.
#   _dirty       (out) boolean; whether or not there are uncommitted changes.
#   _success     (out) boolean; whether or not both
function(GetGitState _working_dir _hashvar _dirty _success)

    # Initialize our returns.
    set(${_hashvar} "GIT-NOTFOUND" PARENT_SCOPE)
    set(${_dirty} "false" PARENT_SCOPE)
    set(${_success} "false" PARENT_SCOPE)

    # Find git.
    if(NOT GIT_FOUND)
        find_package(Git QUIET)
    endif()
    if(NOT GIT_FOUND)
        return()
    endif()

    # Get the hash for HEAD.
    execute_process(COMMAND
            "${GIT_EXECUTABLE}" rev-parse --verify HEAD
            WORKING_DIRECTORY "${_working_dir}"
            RESULT_VARIABLE res
            OUTPUT_VARIABLE hash
            ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT res EQUAL 0)
        # The git command failed.
        return()
    endif()

    # Record the SHA1 hash for HEAD.
    set(${_hashvar} "${hash}" PARENT_SCOPE)

    # Get whether or not the working tree is dirty.
    execute_process(COMMAND
            "${GIT_EXECUTABLE}" status --porcelain
            WORKING_DIRECTORY "${_working_dir}"
            RESULT_VARIABLE res
            OUTPUT_VARIABLE out
            ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT res EQUAL 0)
        # The git command failed.
        return()
    endif()

    # If there were uncommitted changes, mark it as dirty.
    if (NOT "${out}" STREQUAL "")
        set(${_dirty} "true" PARENT_SCOPE)
    else()
        set(${_dirty} "false" PARENT_SCOPE)
    endif()

    # We got this far, so git must have cooperated.
    set(${_success} "true" PARENT_SCOPE)
endfunction()



# Function: GetGitStateSimple
# Description: gets the current state of the git repo and represent it with a string.
# Args:
#   _working_dir (in)  string; the directory from which git commands will be ran.
#   _state       (out) string; describes the current state of the repo.
function(GetGitStateSimple _working_dir _state)

    # Get the current state of the repo where the current list resides.
    GetGitState("${_working_dir}" hash dirty success)

    # We're going to construct a variable that represents the state of the repo.
    set(help_string "\
This is a git state file. \
The next three lines are a success code, SHA1 hash, \
and whether or not there were uncommitted changes.")
    set(${_state} "${help_string}\n${success}\n${hash}\n${dirty}" PARENT_SCOPE)
endfunction()



# Function: MonitorGit
# Description: this function sets up custom commands that make the build system
#              check the state of git before every build. If the state has
#              changed, then a file is configured.
function(MonitorGit)
    add_custom_target(AlwaysCheckGit
            DEPENDS ${pre_configure_file}
            BYPRODUCTS ${post_configure_file}
            COMMAND
            ${CMAKE_COMMAND}
            -DGIT_FUNCTION=DoMonitoring
            -DGIT_WORKING_DIR=${CMAKE_CURRENT_SOURCE_DIR}
            -Dpre_configure_file=${pre_configure_file}
            -Dpost_configure_file=${post_configure_file}
            -P "${CMAKE_CURRENT_LIST_FILE}")
endfunction()



# Function: CheckGit
# Description: check if the git repo has changed. If so, update the state file.
# Args:
#   _working_dir    (in)  string; the directory from which git commands will be ran.
#   _state_changed (out)    bool; whether or no the state of the repo has changed.
function(CheckGit _working_dir _state_changed)

    # Get the state of the repo where the current list resides.
    GetGitStateSimple(${_working_dir} current_state)


    # Check if the state has changed compared to the backup or needs to be
    # regenerated after make clean.
    if(EXISTS "${git_state_file}" AND EXISTS "${post_configure_file}")
        file(READ "${git_state_file}" OLD_HEAD_CONTENTS)
        if(OLD_HEAD_CONTENTS STREQUAL current_state)
            set(${_state_changed} "false" PARENT_SCOPE)
            return()
        endif()
    endif()

    # The state has changed.
    # We need to update the state file.
    file(WRITE "${git_state_file}" "${current_state}")
    set(${_state_changed} "true" PARENT_SCOPE)

endfunction()



# Function: Main
# Description: primary entry-point to the script. Functions are selected based
#              on the GIT_FUNCTION variable.
function(Main)
    if(GIT_FUNCTION STREQUAL DoMonitoring)
        # Check if the repo has changed.
        # If so, run the change action.
        CheckGit("${GIT_WORKING_DIR}" changed)
        if(changed)
            message(STATUS "Checking git... changed!")
            GitStateChangedAction()
        else()
            message(STATUS "Checking git... no change.")
        endif()
    else()
        # Start monitoring git.
        # This should only ever be run once when the module is imported.
        # Behind the scenes, all this does is setup a custom target.
        MonitorGit()
    endif()
endfunction()

# And off we go...
Main()
