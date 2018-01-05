set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} /usr/local/lib)

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


# find playground dependencies
FIND_LIBRARY(UWS_LIBRARY NAME uWS HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib)
FIND_LIBRARY(SSL_LIBRARY NAME ssl HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib)
FIND_LIBRARY(CRYPTO_LIBRARY NAME crypto HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib)
FIND_LIBRARY(UV_LIBRARY NAME uv HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib)


target_link_libraries(
    hyrisePlayground
    hyrise
    hyriseBenchmarkLib
    ${UWS_LIBRARY}
    ${SSL_LIBRARY}
    ${CRYPTO_LIBRARY}
    z
    ${UV_LIBRARY}
)

include_directories(/usr/local/opt/openssl/include)

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
