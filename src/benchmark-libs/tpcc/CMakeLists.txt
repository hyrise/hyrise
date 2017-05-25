# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
    table_generator.cpp
    table_generator.hpp
    random_generator.hpp
    order_status.cpp
    order_status.hpp
)

set(
    LIBRARIES
    opossum
)

# Configure the tpc library
add_library(tpcc STATIC ${SOURCES})
target_link_libraries(tpcc ${LIBRARIES})
