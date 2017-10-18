# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
    defines.hpp
    helper.cpp
    helper.hpp
    new_order.cpp
    new_order.hpp
    order_status.cpp
    order_status.hpp
    tpcc_random_generator.hpp
    tpcc_table_generator.cpp
)

set(
    LIBRARIES
    hyrise
)

# Configure the tpc library
add_library(tpcc STATIC ${SOURCES})
target_link_libraries(tpcc ${LIBRARIES})
