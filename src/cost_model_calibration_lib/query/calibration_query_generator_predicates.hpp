#pragma once

#include <json.hpp>

#include <map>
#include <random>
#include <string>

#include "../configuration/calibration_column_specification.hpp"
#include "../configuration/calibration_table_specification.hpp"

namespace opossum {

using PredicateGeneratorFunctor = std::function<std::optional<std::string>(
    const std::pair<std::string, CalibrationColumnSpecification>&,
    const CalibrationTableSpecification&, const std::string&)>;

using BetweenPredicateGeneratorFunctor = std::function<std::optional<std::pair<std::string, std::string>>(
    const std::pair<std::string, CalibrationColumnSpecification>&,
    const CalibrationTableSpecification&, const std::string&)>;

class CalibrationQueryGeneratorPredicates {
 public:
  static const std::optional<std::string> generate_predicates(
      const PredicateGeneratorFunctor& predicate_generator,
      const CalibrationTableSpecification& table_definition,
      const size_t number_of_predicates, const std::string& predicate_join_keyword = "AND",
      const std::string& column_name_prefix = "");

  /*
   * Functors to generate predicates
   */
  static const std::optional<std::string> generate_between_predicate_value(
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

  static const std::optional<std::string> generate_between_predicate_column(
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

  static const std::optional<std::string> generate_predicate_column_value(
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

  static const std::optional<std::string> generate_predicate_column_column(
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

  static const std::optional<std::string> generate_predicate_like(
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

  static const std::optional<std::string> generate_equi_predicate_for_strings(
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

 private:
  static const std::string _generate_table_scan_predicate_value(
      const CalibrationColumnSpecification& column_definition);

  static const std::optional<std::string> _generate_between(
      const BetweenPredicateGeneratorFunctor& between_predicate_generator,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

  static const std::optional<std::string> _generate_column_predicate(
      const PredicateGeneratorFunctor& predicate_generator,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
      const CalibrationTableSpecification& table_definition,
      const std::string& column_name_prefix);

  CalibrationQueryGeneratorPredicates() = default;
};

}  // namespace opossum
