# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    hyrise
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
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

# -force_load is necessary to include all symbols so that plugins can use them
target_link_libraries(
    hyrisePlayground
    -force_load hyrise
    hyriseBenchmarkLib
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

# -force_load is necessary to include all symbols so that plugins can use them
target_link_libraries(
    hyriseClient
    -force_load hyrise
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
    hyrise
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
