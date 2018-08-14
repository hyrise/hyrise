#pragma once

#include <json.hpp>
#include <map>
#include <string>

#include "calibration_column_specification.hpp"

namespace opossum {

    struct CalibrationTableSpecification {
        CalibrationTableSpecification(
                std::string table_path,
                std::string table_name,
                int table_size,
                std::map<std::string, CalibrationColumnSpecification> columns);

        static CalibrationTableSpecification parse_json_configuration(const nlohmann::json& configuration);

        std::string table_path;
        std::string table_name;
        int table_size;
        std::map<std::string, CalibrationColumnSpecification> columns;
    };

}  // namespace opossum