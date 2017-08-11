# Configure server
add_executable(
    opossumServer

    server_main.cpp
)
target_link_libraries(
    opossumServer
    opossum
    boost_program_options
)

# Configure playground
add_executable(
    opossumPlayground

    playground.cpp
)
target_link_libraries(
    opossumPlayground
    opossum
)

# Configure tpccTableGenerator
add_executable(tpccTableGenerator tpcc_table_generator.cpp)
target_link_libraries(tpccTableGenerator opossum tpcc)

# Configure tpchTableGenerator
add_executable(tpchTableGenerator tpch_table_generator.cpp)
target_link_libraries(tpchTableGenerator opossum tpch)

# Configure client
add_executable(
    opossumClient

    client.cpp
    client.hpp
)
target_link_libraries(
    opossumClient
    opossum
    boost_program_options
)

# Configure Sql_Repl
add_executable(
    opossumRepl

    sql_repl.cpp
    sql_repl.hpp
)
target_link_libraries(
    opossumRepl
    opossum
    tpcc
)

# Configure Console
add_executable(
    opossumConsole

    console.cpp
    console.hpp
)
target_link_libraries(
    opossumConsole
    opossum
    readline
)
