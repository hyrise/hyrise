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

target_link_libraries_system(hyriseCalibration nlohmann_json::nlohmann_json)
