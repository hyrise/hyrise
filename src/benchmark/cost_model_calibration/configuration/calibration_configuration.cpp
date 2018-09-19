#include "calibration_configuration.hpp"

#include <iostream>

#include "calibration_table_specification.hpp"

namespace opossum {

//    CalibrationConfiguration::CalibrationConfiguration(
//            const std::vector<CalibrationTableSpecification> table_specification,
//            const std::string output_path,
//            const int calibration_runs): table_specifications(table_specification), output_path(output_path), calibration_runs(calibration_runs) {}

//    void CalibrationConfiguration::from_json(const nlohmann::json& configuration, CalibrationConfiguration&) {
//      const auto output_path = configuration["output_path"];
//      const auto calibration_runs = configuration["calibration_runs"];
//
//      const auto json_table_specifications = configuration["table_specifications"];
//
//      std::vector<CalibrationTableSpecification> table_specifications;
//      std::transform(
//              json_table_specifications.begin(),
//              json_table_specifications.end(),
//              std::back_inserter(table_specifications),
//              [](const nlohmann::json& table) -> CalibrationTableSpecification {
//                return CalibrationTableSpecification::parse_json_configuration(table);
//              });
//
//      return CalibrationConfiguration(table_specifications, output_path, calibration_runs);
//    }
//
//    const nlohmann::json CalibrationConfiguration::to_json(const nlohmann::json& configuration) {
//        nlohmann::json output_json{};
//        output_json["output_path"] = output_path;
//        output_json["calibration_runs"] = calibration_runs;
//
//        configuration["table_specifications"] = table_specifications
//
//        std::vector<CalibrationTableSpecification> table_specifications;
//        std::transform(
//                json_table_specifications.begin(),
//                json_table_specifications.end(),
//                std::back_inserter(table_specifications),
//                [](const nlohmann::json& table) -> CalibrationTableSpecification {
//                    return CalibrationTableSpecification::parse_json_configuration(table);
//                });
//
//        return output_json;
//    }

}  // namespace opossum
