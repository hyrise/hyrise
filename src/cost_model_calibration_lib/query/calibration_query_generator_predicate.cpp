#include "calibration_query_generator_predicate.hpp"

#include <random>

#include "constant_mappings.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

/**
* Generates a list of predicates connected by AND
*/
const std::shared_ptr<PredicateNode> CalibrationQueryGeneratorPredicate::generate_predicates(
    const PredicateGeneratorFunctor& predicate_generator,
    const std::vector<CalibrationColumnSpecification>& column_definitions, const std::shared_ptr<StoredTableNode>& table,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  const auto filter_column = _find_column_for_configuration(column_definitions, configuration);
  if (!filter_column) {
    std::cout << "Did not find column for configuration "
              << encoding_type_to_string.left.at(configuration.encoding_type) << " "
              << data_type_to_string.left.at(configuration.data_type) << " " << configuration.selectivity << " "
              << configuration.reference_column << std::endl;
    return {};
  }

  auto predicate = predicate_generator(table, *filter_column, configuration);

  // TODO(Sven): add test for this case
  if (!predicate) return {};
  const auto predicate_node = PredicateNode::make(predicate);


//  if (configuration.reference_column) {
//    auto validate_node = ValidateNode::make();
//    predicate_node->set_left_input(validate_node);
//  }

  return predicate_node;
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  const auto& between_predicate_value = [configuration](const std::shared_ptr<StoredTableNode>& table,
                                                        const CalibrationColumnSpecification& filter_column)
      -> std::optional<std::pair<std::shared_ptr<AbstractExpression>, std::shared_ptr<AbstractExpression>>> {
    const auto first_filter_column_value = _generate_value_expression(filter_column, configuration.selectivity);
    const auto second_filter_column_value = _generate_value_expression(filter_column, 0.5f);

    if (!first_filter_column_value || !second_filter_column_value) return {};

    if (first_filter_column_value->value < second_filter_column_value->value) {
      return std::make_pair(first_filter_column_value, second_filter_column_value);
    }
    return std::make_pair(second_filter_column_value, first_filter_column_value);
  };

  return _generate_between(table, between_predicate_value, filter_column);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  const auto& between_predicate_column_functor = [](const std::shared_ptr<StoredTableNode>& table,
                                                    const CalibrationColumnSpecification& filter_column)
      -> std::optional<std::pair<std::shared_ptr<AbstractExpression>, std::shared_ptr<AbstractExpression>>> {
    const auto filter_column_expression = lqp_column_(table->get_column(filter_column.column_name));

    static std::mt19937 engine((std::random_device()()));
    auto remaining_columns = table->get_columns();

    remaining_columns.erase(
        std::remove(remaining_columns.begin(), remaining_columns.end(), filter_column_expression->column_reference));

    std::shuffle(remaining_columns.begin(), remaining_columns.end(), engine);

    std::shared_ptr<AbstractExpression> second_column;
    for (const auto& column : remaining_columns) {
      const auto column_expression = lqp_column_(column);
      if (column_expression->data_type() != filter_column_expression->data_type()) continue;

      // Refactor to use return
      second_column = column_expression;
      remaining_columns.erase(std::remove(remaining_columns.begin(), remaining_columns.end(), column));
      break;
    }

    if (!second_column) return {};

    for (const auto& column : remaining_columns) {
      const auto column_expression = lqp_column_(column);
      if (column_expression->data_type() != second_column->data_type()) continue;

      return std::make_pair(second_column, column_expression);
    }

    return {};
  };

  return _generate_between(table, between_predicate_column_functor, filter_column);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_column_value(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  const auto& filter_column_value_functor = [](const std::shared_ptr<StoredTableNode>& table,
                                               const CalibrationColumnSpecification& filter_column,
                                               const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
    return _generate_value_expression(filter_column, configuration.selectivity);
  };

  return _generate_column_predicate(table, filter_column_value_functor, filter_column, configuration);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_column_column(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  const auto& filter_column_column_functor =
      [](const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
         const CalibrationQueryGeneratorPredicateConfiguration& configuration) -> std::shared_ptr<AbstractExpression> {
    static std::mt19937 engine((std::random_device()()));

    table->column_expressions();

    const auto filter_column_expression = lqp_column_(table->get_column(filter_column.column_name));

    auto columns = table->get_columns();
    std::shuffle(columns.begin(), columns.end(), engine);

    // Find the first column that has the same data type
    for (const auto& column : columns) {
      const auto column_expression = lqp_column_(column);
      if (*column_expression == *filter_column_expression) continue;
      if (column_expression->data_type() != filter_column_expression->data_type()) continue;

      return column_expression;
    }

    return {};
  };

  return _generate_column_predicate(table, filter_column_column_functor, filter_column, configuration);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_like(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  if (filter_column.type != DataType::String) return {};

  const auto lhs = lqp_column_(table->get_column(filter_column.column_name));
  const auto rhs = _generate_value_expression(filter_column, configuration.selectivity, true);

  if (!rhs) {
    return {};
  }

  return like_(lhs, rhs);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  // We just want Equi Scans on String columns for now
  if (filter_column.type != DataType::String) return {};

  const auto lqp_column_reference = table->get_column(filter_column.column_name);
  const auto lhs = lqp_column_(table->get_column(filter_column.column_name));

  // TODO(Sven): Get one existing value from column and filter by that
  const auto stored_table = StorageManager::get().get_table(table->table_name);
  const auto column_id = lqp_column_reference.original_column_id();

  static std::mt19937 engine((std::random_device()()));
  std::uniform_int_distribution<uint64_t> row_number_dist(0, stored_table->row_count() - 1);
  const auto rhs = stored_table->get_value<std::string>(column_id, row_number_dist(engine));
  return equals_(lhs, rhs);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_or(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {

  const CalibrationQueryGeneratorPredicateConfiguration second_configuration {
    configuration.table_name,
    configuration.encoding_type,
    configuration.data_type,
    0.5f,
    configuration.reference_column
    };

  const auto lhs = generate_predicate_column_value(table, filter_column, configuration);
  const auto rhs = generate_predicate_column_value(table, filter_column, second_configuration);

  return or_(lhs, rhs);
}

const std::optional<CalibrationColumnSpecification> CalibrationQueryGeneratorPredicate::_find_column_for_configuration(
    const std::vector<CalibrationColumnSpecification>& column_definitions,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  for (const auto& definition : column_definitions) {
    if (definition.type == configuration.data_type && definition.encoding == configuration.encoding_type) {
      return definition;
    }
  }

  return {};
}

/**
 * Helper function to generate less-than-equal predicate
 * @param table
 * @param predicate_generator
 * @param filter_column
 * @return
 */
const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::_generate_column_predicate(
    const std::shared_ptr<StoredTableNode>& table, const PredicateGeneratorFunctor& predicate_generator,
    const CalibrationColumnSpecification& filter_column,
    const CalibrationQueryGeneratorPredicateConfiguration& configuration) {
  const auto lqp_column_reference = table->get_column(filter_column.column_name);
  const auto lhs = lqp_column_(lqp_column_reference);
  const auto rhs = predicate_generator(table, filter_column, configuration);

  if (!rhs) return {};

  return less_than_equals_(lhs, rhs);
}

/**
     * Helper function to generate between predicate
     * @param table
     * @param between_predicate_generator
     * @param filter_column
     * @return
     */
const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::_generate_between(
    const std::shared_ptr<StoredTableNode>& table, const BetweenPredicateGeneratorFunctor& between_predicate_generator,
    const CalibrationColumnSpecification& filter_column) {
  const auto column = lqp_column_(table->get_column(filter_column.column_name));

  const auto& between_values = between_predicate_generator(table, filter_column);

  if (!between_values) return {};

  return between_(column, between_values->first, between_values->second);
}

const std::shared_ptr<ValueExpression> CalibrationQueryGeneratorPredicate::_generate_value_expression(
    const CalibrationColumnSpecification& column_definition,
    const float selectivity, const bool trailing_like) {
  auto column_type = column_definition.type;

  const auto int_value = static_cast<int>(column_definition.distinct_values * selectivity);
  const auto float_value = selectivity;
  const auto string_value = static_cast<int>(26 * selectivity);

  switch (column_type) {
    case DataType::Int:
    case DataType::Long:
      return value_(int_value);
    case DataType::String: {
      const auto character = std::string(1, 'A' + string_value);
      if (trailing_like) {
        return value_(character + '%');
      }
      return value_(character);
    }
    case DataType::Float:
    case DataType::Double:
      return value_(float_value);
    case DataType::Bool:
    case DataType::Null:
    default:
      Fail("Unsupported data type in CalibrationQueryGeneratorPredicates, found " +
           data_type_to_string.left.at(column_type));
  }
}

}  // namespace opossum
