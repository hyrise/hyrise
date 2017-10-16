# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    hyrise
    boost_program_options
)

# Configure playground
add_executable(
    hyrisePlayground

    playground.cpp
)
target_link_libraries(
    hyrisePlayground
    hyrise
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

    console.cpp
    console.hpp
)
target_link_libraries(
    hyriseConsole
    hyrise
    hyriseBenchmarkLib
    ${READLINE_LIBRARY}
)
target_include_directories(
    hyriseConsole

    PUBLIC
    ${READLINE_INCLUDE_DIR}
)