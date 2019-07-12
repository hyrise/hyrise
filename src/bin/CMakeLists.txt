# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    hyrise
)

# Configure boost server
    add_executable(
    hyriseServerBoost

    start_boost_server.cpp
)
target_link_libraries(
    hyriseServerBoost
    hyrise
    hyriseBenchmarkLib
)
target_include_directories(
    hyriseServerBoost

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
    hyrise
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
    hyriseBenchmarkLib
    ncurses
    ${READLINE_LIBRARY}
)
target_include_directories(
    hyriseConsole

    SYSTEM PUBLIC
    ${READLINE_INCLUDE_DIR}
)
