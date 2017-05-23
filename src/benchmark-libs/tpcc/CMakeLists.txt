# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
        tpcc_table_generator.cpp
)

set(
    LIBRARIES
    opossum
)

# Configure the tpcc library
add_library(tpcc STATIC ${SOURCES})
target_link_libraries(tpcc ${LIBRARIES})
