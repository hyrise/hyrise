# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    hyrise
    hyriseBenchmarkLib
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

    PRIVATE
    hyriseBenchmarkLib
    ncurses
    readline
)
