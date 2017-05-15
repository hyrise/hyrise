# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
    table_generator.cpp
)

set(
    LIBRARIES
    opossum
)

# Configure the tpch library
add_library(tpch STATIC ${SOURCES})
target_link_libraries(tpch ${LIBRARIES})
