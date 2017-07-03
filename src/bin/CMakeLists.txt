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

# Configure statistics playground
add_executable(
    opossumPlaygroundStatistics

    playgroundStatistics.cpp
)
target_link_libraries(
    opossumPlaygroundStatistics
    opossum
    tpcc
)

# Configure tpcTableGenerator
add_executable(tpcTableGenerator tpc_table_generator.cpp)
target_link_libraries(tpcTableGenerator opossum tpcc)

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
