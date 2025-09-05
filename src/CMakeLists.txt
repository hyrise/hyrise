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
    add_compile_options(
        -O0
        -fno-inline
    )

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(
            -fprofile-arcs
            -ftest-coverage
        )
        set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(
            -fprofile-instr-generate
            -fcoverage-mapping
        )
        add_link_options(
            -fprofile-instr-generate
            -fcoverage-mapping
        )
        set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    else()
        message(FATAL_ERROR "Don't know how to run coverage on your compiler (${CMAKE_CXX_COMPILER_ID}).")
    endif()
endif()

# Global flags and include directories
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)

add_compile_options(
    -pthread
    -Wno-unknown-warning-option
)

# Use HYRISE_RELAXED_BUILD to disable strict warnings (e.g., when testing an unsupported compiler or an unsupported
# system)
if (NOT "${HYRISE_RELAXED_BUILD}")
    add_compile_options(
        -pthread
        -Wall
        -Wextra
        -pedantic
        -Werror
        -Wno-deprecated-dynamic-exception-spec # Needed for jemalloc, at least for the older version that we are using.
#        -Wno-dollar-in-identifier-extension
        -Wno-subobject-linkage
        -Wno-unknown-pragmas
        -Wno-unused-parameter
    )

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(
            -Weverything
            -Wno-c++98-compat-pedantic          # We are incompatible with C++98.
            -Wno-ctad-maybe-unsupported         # Warns on some of the class template argument deductions we use. Could
                                                # be fixable.
            -Wno-documentation                  # We do not really do Doxygen documentation.
            -Wno-double-promotion               # We implicitly convert float to double several times.
            -Wno-exit-time-destructors          # We use declarations that require exit-time destructors.
            -Wno-float-equal                    # Currently we compare floats for equality or inequality.
            -Wno-global-constructors            # We use declarations that require global destructors.
            -Wno-header-hygiene                 # Complains about "using namespace hyrise" in our tests and benchmarks
                                                # when a unity builds are activated.
            -Wno-missing-variable-declarations  # Complains about non-static variables. I do not know if fixable.
            -Wno-missing-prototypes             # Messes with our tests and benchmarks.
            -Wno-newline-eof                    # Warns about the thrid_party sql-parser. Please examine.
            -Wno-padded                         # Of course the compiler adds padding to our classes.
            -Wno-pass-failed                    # Complains about failed loop unrolling when building release builds
                                                # with sanitization.
            -Wno-poison-system-directories      # Only fails on the x86 Mac CI stage. Could be worth a closer look.
            -Wno-sign-conversion                # Currently we do many implicit conversions that change signedness.
            -Wno-switch-default                 # Warns if a switch has no default, even if all enum values are
                                                # covered. Contradicts -Wcovered-switch-default.
            -Wno-switch-enum                    # We have switches in which we do not want to cover every enum value
                                                # but use a default.
            -Wno-undefined-func-template        # Complains about missing explicit instantiation declarations. I do not
                                                # know if this is fixable.
            -Wno-unsafe-buffer-usage            # We access buffers via pointers / pointer arithmetic in some cases when
                                                # we know what we do (e.g., uninitialized vector works like this).
            -Wno-weak-vtables                   # Some of our base classes have no out-of-line virtual method
                                                # definitions. I do not know if this is fixable.
        )
    endif()

else()
    message(WARNING "Hyrise will be built with most compiler warnings deactivated. This is fine if you want to test Hyrise but will become an issue when you want to contribute code.")
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 13 AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS_EQUAL 14.99)
        message(WARNING "Disabling 'reserved-identifier' warning for clang 13/14 (version is ${CMAKE_CXX_COMPILER_VERSION}).")
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
    if (NOT "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        message(ERROR "clang-tidy is only available for Clang builds.")
    endif()

    # Get the full clang-tidy path (helps with MacOS builds).
    set(CMAKE_CXX_CLANG_TIDY "${CMAKE_C_COMPILER}-tidy")
    message(STATUS "clang-tidy enabled")
endif()

add_subdirectory(benchmarklib)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(plugins)

# No clang-tidy for the following subfolders:
set(CMAKE_CXX_CLANG_TIDY "")
add_subdirectory(benchmark)
add_subdirectory(test)
