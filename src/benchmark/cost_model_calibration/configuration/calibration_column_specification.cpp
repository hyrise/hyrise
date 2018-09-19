#include "calibration_column_specification.hpp"

#include "constant_mappings.hpp"

namespace opossum {

//    CalibrationColumnSpecification::CalibrationColumnSpecification(
//            const std::string type,
//            const std::string value_distribution,
//            const bool sorted,
//            const int distinct_values,
//            const EncodingType encoding): type(type), value_distribution(value_distribution), sorted(sorted), distinct_values(distinct_values), encoding(encoding) {}

//    CalibrationColumnSpecification CalibrationColumnSpecification::parse_json_configuration(const nlohmann::json& configuration) {
//      const auto type = configuration["type"];
//      const auto value_distribution = configuration["value_distribution"];
//      const auto sorted = configuration.value("sorted", false);
//      const auto distinct_values = configuration["distinct_values"];
//      const auto encoding_string = configuration.value("encoding", "Dictionary");
//
//      if (encoding_type_to_string.right.find(encoding_string) == encoding_type_to_string.right.end()) {
//        Fail("Unsupported encoding");
//      }
//      const auto mapped_encoding = encoding_type_to_string.right.at(encoding_string);
//
//      return CalibrationColumnSpecification(type, value_distribution, sorted, distinct_values, mapped_encoding);
//    }
}  // namespace opossum