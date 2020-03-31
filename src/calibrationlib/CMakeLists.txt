include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/third_party
)

# Configure calibrationlib
set(
    SOURCES

    calibration_benchmark_runner.cpp
    calibration_benchmark_runner.hpp
    calibration_table_wrapper.cpp
    calibration_table_wrapper.hpp
    csv_writer.cpp
    csv_writer.hpp
    calibration_lqp_generator.cpp
    calibration_lqp_generator.hpp
    operator_feature_export.cpp
    operator_feature_export.hpp
    table_feature_export.cpp
    table_feature_export.hpp
    calibration_table_generator.cpp
    calibration_table_generator.hpp
)

add_library(hyriseCalibrationLib ${SOURCES})

target_link_libraries(
    hyriseCalibrationLib
    hyrise
    hyriseBenchmarkLib
)
