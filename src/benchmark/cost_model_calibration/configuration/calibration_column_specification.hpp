#pragma once

#include <json.hpp>
#include <string>

namespace opossum {

    struct CalibrationColumnSpecification {
        CalibrationColumnSpecification(
                const std::string type,
                const std::string value_distribution,
                const bool sorted,
                const int distinct_values,
                const std::string encoding);

        static CalibrationColumnSpecification parse_json_configuration(const nlohmann::json& configuration);

        std::string type;
        std::string value_distribution;
        bool sorted;
        int distinct_values;
        std::string encoding;
    };

}  // namespace opossum