include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(
        SOURCES

        configuration/calibration_column_specification.hpp
        configuration/calibration_configuration.hpp
        configuration/calibration_table_specification.hpp
        cost_model_calibration.cpp
        cost_model_calibration.hpp
        cost_model_calibration_query_runner.cpp
        cost_model_calibration_query_runner.hpp
        cost_model_calibration_table_generator.cpp
        cost_model_calibration_table_generator.hpp
        cost_model_feature_extractor.cpp
        cost_model_feature_extractor.hpp
        feature/abstract_features.cpp
        feature/abstract_features.hpp
        feature/aggregate_features.cpp
        feature/aggregate_features.hpp
        feature/column_features.cpp
        feature/column_features.hpp
        feature/constant_hardware_features.cpp
        feature/constant_hardware_features.hpp
        feature/calibration_features.cpp
        feature/calibration_features.hpp
        feature/join_features.cpp
        feature/join_features.hpp
        feature/projection_features.cpp
        feature/projection_features.hpp
        feature/runtime_hardware_features.cpp
        feature/runtime_hardware_features.hpp
        feature/table_scan_features.cpp
        feature/table_scan_features.hpp
        query/calibration_query_generator.cpp
        query/calibration_query_generator.hpp
        query/calibration_query_generator_aggregates.cpp
        query/calibration_query_generator_aggregates.hpp
        query/calibration_query_generator_join.cpp
        query/calibration_query_generator_join.hpp
        query/calibration_query_generator_predicate.cpp
        query/calibration_query_generator_predicate.hpp
        query/calibration_query_generator_projection.cpp
        query/calibration_query_generator_projection.hpp
)


# Configure the regular opossum library used for tests/server/playground...
add_library(hyriseCostModelCalibrationLib STATIC ${SOURCES})

# Add this magic dependency to run the git_watcher on build
add_dependencies(hyriseCostModelCalibrationLib AlwaysCheckGit)

target_link_libraries(hyriseCostModelCalibrationLib hyrise hyriseBenchmarkLib ${FILESYSTEM_LIBRARY})
