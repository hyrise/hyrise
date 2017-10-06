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

# Configure Console
add_executable(
    opossumConsole

    console/console.cpp
    console/console.hpp
    console/table_printer.cpp
    console/table_printer.hpp
)
target_link_libraries(
    opossumConsole
    opossum
    tpcc
    ncurses
    ${READLINE_LIBRARY}
)
target_include_directories(
    opossumConsole

    PUBLIC
    ${READLINE_INCLUDE_DIR}
)