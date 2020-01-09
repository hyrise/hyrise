# Configure calibration
add_executable(
    hyriseCalibration

    calibration.cpp
    calibration_table_wrapper.cpp
    calibration_table_wrapper.hpp
    lqp_generator.cpp
    lqp_generator.hpp
    measurement_export.cpp
    measurement_export.hpp
    table_generator.cpp
    table_generator.hpp
)

target_link_libraries(
    hyriseCalibration
    hyrise
    hyriseBenchmarkLib
)


# Configure Console
# add_executable(
#     hyriseConsole

#     console/console.cpp
#     console/console.hpp
#     console/pagination.cpp
#     console/pagination.hpp
# )
# target_link_libraries(
#     hyriseConsole
#     hyriseBenchmarkLib
#     ncurses
#     ${READLINE_LIBRARY}
# )
# target_link_libraries_system(hyriseConsole nlohmann_json::nlohmann_json)
# target_include_directories(
#     hyriseConsole

#     SYSTEM PUBLIC
#     ${READLINE_INCLUDE_DIR}
# )
