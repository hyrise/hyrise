#pragma once

#include <json.hpp>
#include <string>
#include <storage/encoding_type.hpp>

namespace opossum {

    struct CalibrationColumnSpecification {
        CalibrationColumnSpecification(
                std::string type,
                std::string value_distribution,
                bool sorted,
                int distinct_values,
                EncodingType encoding);

        static CalibrationColumnSpecification parse_json_configuration(const nlohmann::json& configuration);

        std::string type;
        std::string value_distribution;
        bool sorted;
        int distinct_values;
        EncodingType encoding;
    };

}  // namespace opossum