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
    operator_feature_exporter.cpp
    operator_feature_exporter.hpp
    table_feature_exporter.cpp
    table_feature_exporter.hpp
)

add_library(hyriseCalibrationLib ${SOURCES})

target_link_libraries(
    hyriseCalibrationLib

    PUBLIC
    hyrise
    hyriseBenchmarkLib
)
