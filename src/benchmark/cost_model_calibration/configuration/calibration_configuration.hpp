#pragma once

#include <json.hpp>
#include <map>
#include <string>
#include <vector>

#include "calibration_table_specification.hpp"

namespace opossum {

    struct CalibrationConfiguration {

        CalibrationConfiguration(
                std::vector<CalibrationTableSpecification> table_specification,
                std::string output_path,
                int calibration_runs);

        static CalibrationConfiguration parse_json_configuration(const nlohmann::json& configuration);

        std::vector<CalibrationTableSpecification> table_specifications;
        std::string output_path;
        size_t calibration_runs;

    };
}  // namespace opossum