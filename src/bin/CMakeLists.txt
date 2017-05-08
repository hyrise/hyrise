# Configure server
add_executable(
    opossumServer
    server_main.cpp
)
target_link_libraries(opossumServer opossum boost_program_options)

# Configure playground
add_executable(
    opossumPlayground
    playground.cpp
    table_generator.cpp
    table_generator.hpp
)
target_link_libraries(opossumPlayground opossum)

# Configure tpcc playground
add_executable(
        opossumPlaygroundTPCC
        playgroundTPCCBenchmark.cpp
        ../benchmark/tpcc/tpcc_table_generator.cpp
)
target_link_libraries(opossumPlaygroundTPCC opossum)

# Configure client
add_executable(
    opossumClient
    client.cpp
    client.hpp
)
target_link_libraries(opossumClient opossum boost_program_options)


