# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
        text_field_generator.cpp
        tpch_grammer.cpp
        tpch_table_generator.cpp
)

set(
    LIBRARIES
    hyrise
)

# Configure the tpch library
add_library(tpch STATIC ${SOURCES})
target_link_libraries(tpch ${LIBRARIES})
