#pragma once

#include <json.hpp>

#include <string>
#include <vector>

#include "../configuration/calibration_column_specification.hpp"
#include "../configuration/calibration_table_specification.hpp"
#include "calibration_query_generator_predicates.hpp"

namespace opossum {

class CalibrationQueryGenerator {
 public:
  static const std::vector<std::string> generate_queries(
      const std::vector<CalibrationTableSpecification>& table_definitions);

 private:
  static const std::optional<std::string> _generate_table_scan(const CalibrationTableSpecification& table_definition,
                                                               const PredicateGeneratorFunctor& predicate_generator,
                                                               const std::string& predicate_join_keyword = "AND");
  static const std::optional<std::string> _generate_aggregate(const CalibrationTableSpecification& table_definition);
  static const std::optional<std::string> _generate_join(
      const std::vector<CalibrationTableSpecification>& table_definitions);
  static const std::optional<std::string> _generate_foreign_key_join(
      const std::vector<CalibrationTableSpecification>& table_definitions);

  static const std::string _generate_select_columns(
      const std::map<std::string, CalibrationColumnSpecification>& column_definitions);

  static const std::optional<std::pair<std::pair<std::string, CalibrationColumnSpecification>,
                                       std::pair<std::string, CalibrationColumnSpecification>>>
  _generate_join_columns(const std::map<std::string, CalibrationColumnSpecification>& left_column_definitions,
                         const std::map<std::string, CalibrationColumnSpecification>& right_column_definitions);

  static const std::vector<std::string> _get_column_names(
      const std::map<std::string, CalibrationColumnSpecification>& column_definitions);

  CalibrationQueryGenerator() = default;
};

}  // namespace opossum
