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

# Configure tpcc playground
add_executable(
    opossumPlaygroundTPCC

    playgroundTPCCBenchmark.cpp
)
target_link_libraries(
    opossumPlaygroundTPCC
    opossum
    tpcc
)

# Configure tpch playground
add_executable(
    opossumPlaygroundTPCH

    playgroundTPCHBenchmark.cpp
)
target_link_libraries(
    opossumPlaygroundTPCH
    opossum
    tpch
)

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
