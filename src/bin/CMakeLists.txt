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

    EXCLUDE_FROM_ALL

    playground.cpp
)
target_link_libraries(
    opossumPlayground
    opossum
)

# Configure tpccTableGenerator
add_executable(tpccTableGenerator EXCLUDE_FROM_ALL tpcc_table_generator.cpp)
target_link_libraries(tpccTableGenerator opossum tpcc)

# Configure tpchTableGenerator
add_executable(tpchTableGenerator EXCLUDE_FROM_ALL tpch_table_generator.cpp)
target_link_libraries(tpchTableGenerator opossum tpch)

# Configure client
add_executable(
    opossumClient

    EXCLUDE_FROM_ALL

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

    console.cpp
    console.hpp
)
target_link_libraries(
    opossumConsole
    opossum
    tpcc
    ${READLINE_LIBRARY}
)
target_include_directories(
    opossumConsole

    PUBLIC
    ${READLINE_INCLUDE_DIR}
)