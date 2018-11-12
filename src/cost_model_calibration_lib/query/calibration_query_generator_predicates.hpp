#pragma once

#include <map>
#include <string>

#include "../configuration/calibration_column_specification.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

using PredicateGeneratorFunctor = std::function<std::shared_ptr<AbstractExpression>(
    const std::shared_ptr<StoredTableNode>&, const std::pair<std::string, CalibrationColumnSpecification>&)>;

class CalibrationQueryGeneratorPredicates {
 public:
  static const std::shared_ptr<PredicateNode> generate_predicates(
      const PredicateGeneratorFunctor& predicate_generator,
      const std::map<std::string, CalibrationColumnSpecification>& column_definitions,
      const std::shared_ptr<StoredTableNode>& table, const size_t number_of_predicates);

  /*
   * Functors to generate predicates.
   * They all implement 'PredicateGeneratorFunctor'
   */
  static const std::shared_ptr<AbstractExpression> generate_predicate_between_value_value(
      const std::shared_ptr<StoredTableNode>& table,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  static const std::shared_ptr<AbstractExpression> generate_predicate_between_column_column(
      const std::shared_ptr<StoredTableNode>& table,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  static const std::shared_ptr<AbstractExpression> generate_predicate_column_value(
      const std::shared_ptr<StoredTableNode>& table,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  static const std::shared_ptr<AbstractExpression> generate_predicate_column_column(
      const std::shared_ptr<StoredTableNode>& table,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  static const std::shared_ptr<AbstractExpression> generate_predicate_like(
      const std::shared_ptr<StoredTableNode>& table,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  static const std::shared_ptr<AbstractExpression> generate_predicate_equi_on_strings(
      const std::shared_ptr<StoredTableNode>& table,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  static const std::shared_ptr<AbstractExpression> generate_predicate_or(
      const std::shared_ptr<StoredTableNode>& table,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

 private:
  using BetweenPredicateGeneratorFunctor =
      std::function<std::optional<std::pair<std::shared_ptr<AbstractExpression>, std::shared_ptr<AbstractExpression>>>(
          const std::shared_ptr<StoredTableNode>&, const std::pair<std::string, CalibrationColumnSpecification>&)>;

  static const std::shared_ptr<ValueExpression> _generate_value_expression(
      const CalibrationColumnSpecification& column_definition, const bool trailing_like = false);

  static const std::shared_ptr<AbstractExpression> _generate_between(
      const std::shared_ptr<StoredTableNode>& table, const BetweenPredicateGeneratorFunctor& predicate_generator,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  static const std::shared_ptr<AbstractExpression> _generate_column_predicate(
      const std::shared_ptr<StoredTableNode>& table, const PredicateGeneratorFunctor& predicate_generator,
      const std::pair<std::string, CalibrationColumnSpecification>& filter_column);

  CalibrationQueryGeneratorPredicates() = default;
};

}  // namespace opossum
