#pragma once

#include <json.hpp>

#include <string>
#include <vector>

#include "../configuration/calibration_column_specification.hpp"
#include "../configuration/calibration_table_specification.hpp"

namespace opossum {

class CalibrationQueryGeneratorPredicates {
 public:
  static const std::string generate_predicate(
      const std::map<std::string, CalibrationColumnSpecification>& column_definitions,
      const std::string column_name_prefix = "");

 private:
  static const std::string _generate_table_scan_predicate_value(
      const CalibrationColumnSpecification& column_definition);

  CalibrationQueryGeneratorPredicates() = default;
};

}  // namespace opossum
