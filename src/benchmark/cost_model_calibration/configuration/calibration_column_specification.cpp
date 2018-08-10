#include "calibration_column_specification.hpp"

namespace opossum {

    CalibrationColumnSpecification::CalibrationColumnSpecification(
            const std::string type,
            const std::string value_distribution,
            const bool sorted,
            const int distinct_values,
            const std::string encoding): type(type), value_distribution(value_distribution), sorted(sorted), distinct_values(distinct_values), encoding(encoding) {}

    CalibrationColumnSpecification CalibrationColumnSpecification::parse_json_configuration(const nlohmann::json& configuration) {
      const auto type = configuration["type"];
      const auto value_distribution = configuration["value_distribution"];
      const auto sorted = configuration.value("sorted", false);
      const auto distinct_values = configuration["distinct_values"];
      const auto encoding = configuration.value("encoding", "dictionary");

      return CalibrationColumnSpecification(type, value_distribution, sorted, distinct_values, encoding);
    }

}  // namespace opossum