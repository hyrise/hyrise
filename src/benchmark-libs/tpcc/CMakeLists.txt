# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
    random_generator.hpp
    order_status.cpp
    order_status.hpp
    tpcc_table_generator.cpp
)

set(
    LIBRARIES
    opossum
)

# Configure the tpc library
add_library(tpcc STATIC ${SOURCES})
target_link_libraries(tpcc ${LIBRARIES})
