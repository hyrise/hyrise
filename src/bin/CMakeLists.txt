# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    hyrise
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_THREAD_LIBRARY}
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
    hyrise
    hyriseBenchmarkLib
    ncurses
    ${READLINE_LIBRARY}
    ${FILESYSTEM_LIBRARY}
)
target_include_directories(
    hyriseConsole

    PUBLIC
    ${READLINE_INCLUDE_DIR}
)
