if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(HYRISE_DEBUG 1)
else()
    set(HYRISE_DEBUG 0)
endif()

add_definitions(-DHYRISE_DEBUG=${HYRISE_DEBUG})

# Provide ENABLE_NUMA_SUPPORT option and automatically disable NUMA if libNUMA was not found
option(ENABLE_NUMA_SUPPORT "Build with NUMA support" ON)
if (NOT ${NUMA_FOUND})
    set(ENABLE_NUMA_SUPPORT OFF)
endif()

if (${ENABLE_NUMA_SUPPORT})
    add_definitions(-DHYRISE_NUMA_SUPPORT=1)
    MESSAGE(STATUS "Building with NUMA support")
else()
    add_definitions(-DHYRISE_NUMA_SUPPORT=0)
    MESSAGE(STATUS "Building without NUMA support")
endif()

# Enable coverage if requested - this is only operating on Hyrise's source (src/) so we don't check coverage of
# third_party stuff
option(ENABLE_COVERAGE "Set to ON to build Hyrise with enabled coverage checking. Default: OFF" OFF)
if (${ENABLE_COVERAGE})
    add_compile_options(-O0 -fno-inline)

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fprofile-arcs -ftest-coverage)
        set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
        set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    else()
        message(FATAL_ERROR "Don't know how to run coverage on your compiler (${CMAKE_CXX_COMPILER_ID}).")
    endif()
endif()

# Global flags and include directories
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)

# Hotfix for boost bug (https://github.com/boostorg/asio/issues/312):
add_compile_options(-DBOOST_ASIO_DISABLE_CONCEPTS)

add_compile_options(-pthread -Wno-unknown-warning-option)

# Use HYRISE_RELAXED_BUILD to disable strict warnings (e.g., when testing an unsupported compiler or an unsupported system)
if (NOT "${HYRISE_RELAXED_BUILD}")
    add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas -Wno-subobject-linkage -Wno-deprecated-dynamic-exception-spec)

    # -Wno-deprecated-dynamic-exception-spec is needed for jemalloc, at least for the older version that we are using

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-Weverything -Wshadow-all -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-documentation -Wno-padded -Wno-global-constructors -Wno-sign-conversion -Wno-exit-time-destructors -Wno-switch-enum -Wno-weak-vtables -Wno-double-promotion -Wno-covered-switch-default -Wno-unused-macros -Wno-newline-eof -Wno-missing-variable-declarations -Wno-weak-template-vtables -Wno-missing-prototypes -Wno-float-equal -Wno-return-std-move-in-c++11 -Wno-unreachable-code-break -Wno-undefined-func-template -Wno-unknown-warning-option -Wno-pass-failed -Wno-ctad-maybe-unsupported -Wno-header-hygiene -Wno-poison-system-directories -Wno-zero-as-null-pointer-constant)
    endif()

else()
    message(WARNING "Hyrise will be built with most compiler warnings deactivated. This is fine if you want to test Hyrise but will become an issue when you want to contribute code.")
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 13 AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS_EQUAL 13.99)
        message(WARNING "Disabling 'reserved-identifier' warning for clang 13 (version is ${CMAKE_CXX_COMPILER_VERSION}).")
        add_compile_options(-Wno-reserved-identifier)
    endif()
endif()

include(${PROJECT_SOURCE_DIR}/cmake/TargetLinkLibrariesSystem.cmake)

include_directories(
    ${PROJECT_SOURCE_DIR}/src/benchmarklib/
    ${PROJECT_SOURCE_DIR}/src/lib/
    ${PROJECT_SOURCE_DIR}/src/plugins/
)

set(ENABLE_CLANG_TIDY OFF CACHE BOOL "Run clang-tidy")
if (ENABLE_CLANG_TIDY)
    message(STATUS "clang-tidy enabled")
    set(CMAKE_CXX_CLANG_TIDY "${CMAKE_SOURCE_DIR}/scripts/clang_tidy_wrapper.sh;${CMAKE_SOURCE_DIR}")
endif()

add_subdirectory(benchmarklib)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(plugins)

# No clang-tidy for the following subfolders
set(CMAKE_CXX_CLANG_TIDY "")
add_subdirectory(benchmark)
add_subdirectory(test)
