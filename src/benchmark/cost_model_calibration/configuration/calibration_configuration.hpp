#pragma once

#include <json.hpp>
#include <map>
#include <string>
#include <vector>

#include "calibration_table_specification.hpp"

namespace opossum {

    struct CalibrationConfiguration {

//        CalibrationConfiguration(
//                std::vector<CalibrationTableSpecification> table_specification,
//                std::string output_path,
//                int calibration_runs);

//        static CalibrationConfiguration parse_json_configuration(const nlohmann::json& configuration);

        std::vector<CalibrationTableSpecification> table_specifications;
        std::string output_path;
        size_t calibration_runs;
    };

    inline void to_json(nlohmann::json& j, const CalibrationConfiguration& s) {
        j = nlohmann::json{
                {"output_path", s.output_path},
                {"calibration_runs", s.calibration_runs},
                {"table_specifications", s.table_specifications}
        };
    }

    inline void from_json(const nlohmann::json& j, CalibrationConfiguration& s) {
        s.output_path = j.value("output_path", "./calibration_results.json");
        s.calibration_runs = j.value("calibration_runs", 1000);
        s.table_specifications = j.value("table_specifications", std::vector<CalibrationTableSpecification>{});
    }
}  // namespace opossum