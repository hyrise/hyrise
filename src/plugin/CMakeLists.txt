# Configure TestPlugin
add_library(TestPlugin SHARED
    test_plugin.cpp
    test_plugin.hpp
)
# Prevent the linker under macOS from complaining about undefined methods
if (APPLE)
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        target_link_libraries(TestPlugin PRIVATE "-undefined dynamic_lookup")
    endif()
endif()

# Configure NonInstantiablePlugin
add_library(NonInstantiablePlugin SHARED
    non_instantiable_plugin.cpp
)
# Prevent the linker under macOS from complaining about undefined methods
if (APPLE)
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        target_link_libraries(NonInstantiablePlugin PRIVATE "-undefined dynamic_lookup")
    endif()
endif()

# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")