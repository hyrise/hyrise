#pragma once

#include <map>
#include <string>

#include "../configuration/calibration_column_specification.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace opossum {

struct CalibrationQueryGeneratorPredicateConfiguration {
  const std::string table_name;
  const EncodingType encoding_type;
  const DataType data_type;
  const float selectivity;
  const bool reference_column;
};

using PredicateGeneratorFunctor = std::function<const std::shared_ptr<AbstractExpression>(
    const std::shared_ptr<StoredTableNode>&, const CalibrationColumnSpecification&,
    const CalibrationQueryGeneratorPredicateConfiguration&)>;

class CalibrationQueryGeneratorPredicate {
 public:
  static const std::shared_ptr<PredicateNode> generate_predicates(
      const PredicateGeneratorFunctor& predicate_generator,
      const std::vector<CalibrationColumnSpecification>& column_definitions,
      const std::shared_ptr<StoredTableNode>& table,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  /*
   * Functors to generate predicates.
   * They all implement 'PredicateGeneratorFunctor'
   */
  static const std::shared_ptr<AbstractExpression> generate_predicate_between_value_value(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_between_column_column(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_column_value(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_column_column(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_columnm,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_like(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_equi_on_strings(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_or(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

 private:
  using BetweenPredicateGeneratorFunctor =
      std::function<std::optional<std::pair<std::shared_ptr<AbstractExpression>, std::shared_ptr<AbstractExpression>>>(
          const std::shared_ptr<StoredTableNode>&, const CalibrationColumnSpecification&)>;

  static const std::optional<CalibrationColumnSpecification> _find_column_for_configuration(
      const std::vector<CalibrationColumnSpecification>& column_definitions,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  static const std::shared_ptr<ValueExpression> _generate_value_expression(
      const CalibrationColumnSpecification& column_definition,
      const float selectivity, const bool trailing_like = false);

  static const std::shared_ptr<AbstractExpression> _generate_between(
      const std::shared_ptr<StoredTableNode>& table, const BetweenPredicateGeneratorFunctor& between_predicate_generator,
      const CalibrationColumnSpecification& filter_column);

  static const std::shared_ptr<AbstractExpression> _generate_column_predicate(
      const std::shared_ptr<StoredTableNode>& table, const PredicateGeneratorFunctor& predicate_generator,
      const CalibrationColumnSpecification& filter_column,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  CalibrationQueryGeneratorPredicate() = default;
};

}  // namespace opossum
