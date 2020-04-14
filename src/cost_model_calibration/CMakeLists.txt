include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(hyriseCostModelCalibration cost_model_calibration_main.cpp)
target_link_libraries(
        hyriseCostModelCalibration

        hyrise
        hyriseCostModelCalibrationLib
        hyriseBenchmarkLib
)

target_link_libraries_system(hyriseCostModelCalibration)
