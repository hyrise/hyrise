# -force_load/--whole-archive are necessary to include all symbols so that dynamically loaded plugins can use them
if (APPLE)
    set(HYRISE_LIBRARY_WITH_ALL_SYMBOLS -force_load hyrise)
else()
    set(HYRISE_LIBRARY_WITH_ALL_SYMBOLS -Wl,--whole-archive hyrise -Wl,--no-whole-archive)
endif()

# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    ${HYRISE_LIBRARY_WITH_ALL_SYMBOLS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    ${CMAKE_DL_LIBS}
)
target_include_directories(
    hyriseServer

    PUBLIC
    ${Boost_INCLUDE_DIRS}
)

# Configure playground
add_executable(
    hyrisePlayground

    playground.cpp
)

target_link_libraries(
    hyrisePlayground
    ${HYRISE_LIBRARY_WITH_ALL_SYMBOLS}
    ${CMAKE_DL_LIBS}
)

# Configure tpchTableGenerator
add_executable(tpchTableGenerator tpch_table_generator.cpp)
target_link_libraries(tpchTableGenerator hyrise hyriseBenchmarkLib)

# Configure client
add_executable(
    hyriseClient
    client.cpp
    client.hpp
)

target_link_libraries(
    hyriseClient
    hyrise
)

# Configure Console
add_executable(
    hyriseConsole

    console/console.cpp
    console/console.hpp
    console/pagination.cpp
    console/pagination.hpp
)
target_link_libraries(
    hyriseConsole
    ${HYRISE_LIBRARY_WITH_ALL_SYMBOLS}
    hyriseBenchmarkLib
    ncurses
    ${READLINE_LIBRARY}
    ${CMAKE_DL_LIBS}
)
target_include_directories(
    hyriseConsole

    SYSTEM PUBLIC
    ${READLINE_INCLUDE_DIR}
)
