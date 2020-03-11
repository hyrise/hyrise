# Configure calibration
add_executable(
    hyriseCalibration

    benchmark_builder.cpp
    benchmark_builder.hpp
    calibration.cpp
    calibration_table_wrapper.cpp
    calibration_table_wrapper.hpp
    csv_writer.cpp
    csv_writer.hpp
    lqp_generator.cpp
    lqp_generator.hpp
    measurement_export.cpp
    measurement_export.hpp
    table_export.cpp
    table_export.hpp
    table_generator.cpp
    table_generator.hpp
)

target_link_libraries(
    hyriseCalibration
    hyrise
    hyriseBenchmarkLib
)

target_link_libraries_system(hyriseCalibration nlohmann_json::nlohmann_json)
