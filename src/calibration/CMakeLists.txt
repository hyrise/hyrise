include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Configure calibration
add_executable(
    hyriseCalibration

    calibration.cpp
)

target_link_libraries(
    hyriseCalibration
    hyrise
    hyriseCalibrationLib
)
