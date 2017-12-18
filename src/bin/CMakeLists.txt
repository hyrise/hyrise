# Configure server
add_executable(
    hyriseServer

    server.cpp
    server/hyrise_server.cpp
    server/hyrise_server.hpp
    server/hyrise_session.cpp
    server/hyrise_session.hpp
    server/postgres_wire_handler.cpp
    server/postgres_wire_handler.hpp
    server/types.hpp
    server/commands/abstract_command.cpp
    server/commands/abstract_command.hpp
    server/commands/simple_query_command.cpp
    server/commands/simple_query_command.hpp
    server/commands/startup_command.cpp
    server/commands/startup_command.hpp
    server/commands/terminate_command.cpp
    server/commands/terminate_command.hpp
)
target_link_libraries(
    hyriseServer
#    hyrise
    ${Boost_SYSTEM_LIBRARY}
    boost_program_options
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
    hyrise
    hyriseBenchmarkLib
)

# Configure tpccTableGenerator
add_executable(tpccTableGenerator tpcc_table_generator.cpp)
target_link_libraries(tpccTableGenerator hyrise hyriseBenchmarkLib)

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
    boost_program_options
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
)
target_include_directories(
    hyriseConsole

    PUBLIC
    ${READLINE_INCLUDE_DIR}
)
