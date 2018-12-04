#pragma once

#include <map>
#include <string>

#include "../configuration/calibration_column_specification.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

struct CalibrationQueryGeneratorPredicateConfiguration {
  const std::string table_name;
  const EncodingType first_encoding_type;
  const DataType first_data_type;
  const EncodingType second_encoding_type;
  const DataType second_data_type;
  const EncodingType third_encoding_type;
  const DataType third_data_type;
  const float selectivity;
  const bool reference_column;
  const size_t row_count;
};

struct PredicateGeneratorFunctorConfiguration {
  const std::shared_ptr<StoredTableNode>& table;
  const std::vector<CalibrationColumnSpecification>& column_definitions;
  const CalibrationQueryGeneratorPredicateConfiguration& configuration;
};

using PredicateGeneratorFunctor =
    std::function<const std::shared_ptr<AbstractExpression>(const PredicateGeneratorFunctorConfiguration&)>;

class CalibrationQueryGeneratorPredicate {
 public:
  static const std::vector<std::shared_ptr<PredicateNode>> generate_predicates(
      const PredicateGeneratorFunctor& predicate_generator,
      const std::vector<CalibrationColumnSpecification>& column_definitions,
      const std::shared_ptr<StoredTableNode>& table,
      const CalibrationQueryGeneratorPredicateConfiguration& configuration);

  /*
   * Functors to generate predicates.
   * They all implement 'PredicateGeneratorFunctor'
   */
  static const std::shared_ptr<AbstractExpression> generate_predicate_between_value_value(
      const PredicateGeneratorFunctorConfiguration& generator_configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_between_column_column(
      const PredicateGeneratorFunctorConfiguration& generator_configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_column_value(
      const PredicateGeneratorFunctorConfiguration& generator_configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_column_column(
      const PredicateGeneratorFunctorConfiguration& generator_configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_like(
      const PredicateGeneratorFunctorConfiguration& generator_configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_equi_on_strings(
      const PredicateGeneratorFunctorConfiguration& generator_configuration);

  static const std::shared_ptr<AbstractExpression> generate_predicate_or(
      const PredicateGeneratorFunctorConfiguration& generator_configuration);

 private:
  static const std::optional<CalibrationColumnSpecification> _find_column_for_configuration(
      const std::vector<CalibrationColumnSpecification>& column_definitions, const DataType& data_type,
      const EncodingType& encoding_type);

  static const std::shared_ptr<ValueExpression> _generate_value_expression(const DataType& data_type,
                                                                           const float selectivity,
                                                                           const bool trailing_like = false);

  static const std::shared_ptr<LQPColumnExpression> _generate_column_expression(
      const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column);

  CalibrationQueryGeneratorPredicate() = default;
};

}  // namespace opossum
