#include "calibration_query_generator_predicate.hpp"

#include <random>

#include "expression/expression_functional.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

/**
* Generates a list of predicates connected by AND
*/
const std::shared_ptr<PredicateNode> CalibrationQueryGeneratorPredicates::generate_predicates(
    const PredicateGeneratorFunctor& predicate_generator,
    const std::map<std::string, CalibrationColumnSpecification>& column_definitions,
    const std::shared_ptr<StoredTableNode>& table, const size_t number_of_predicates) {
  static std::mt19937 engine((std::random_device()()));

  // We want to scan on each column at most once
  auto remaining_column_definitions = column_definitions;

  std::vector<std::shared_ptr<PredicateNode>> predicates{};
  for (size_t i = 0; i < number_of_predicates; i++) {
    if (remaining_column_definitions.empty()) continue;

    // select scan column at random
    std::uniform_int_distribution<long> filter_column_dist(0, remaining_column_definitions.size() - 1);
    auto filter_column = std::next(remaining_column_definitions.begin(), filter_column_dist(engine));
    auto predicate = predicate_generator(table, *filter_column);

    if (!predicate) continue;

    predicates.push_back(PredicateNode::make(predicate));

    // Avoid filtering on the same column twice
    remaining_column_definitions.erase(filter_column->first);
  }

  if (predicates.empty()) return {};

  // Construct valid chain of PredicateNodes
  auto current_predicate = predicates.front();
  current_predicate->set_left_input(table);
  for (size_t idx = 1; idx < predicates.size(); ++idx) {
    predicates[idx]->set_left_input(current_predicate);
    current_predicate = predicates[idx];
  }

  return current_predicate;
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::generate_predicate_between_value_value(
    const std::shared_ptr<StoredTableNode>& table,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  const auto& between_predicate_value = [](const std::shared_ptr<StoredTableNode>& table,
                                           const std::pair<std::string, CalibrationColumnSpecification>& filter_column)
      -> std::optional<std::pair<std::shared_ptr<AbstractExpression>, std::shared_ptr<AbstractExpression>>> {
    const auto first_filter_column_value =
        CalibrationQueryGeneratorPredicates::_generate_value_expression(filter_column.second);
    const auto second_filter_column_value =
        CalibrationQueryGeneratorPredicates::_generate_value_expression(filter_column.second);

    if (!first_filter_column_value || !second_filter_column_value) return {};

    if (first_filter_column_value < second_filter_column_value) {
      return std::make_pair(first_filter_column_value, second_filter_column_value);
    }
    return std::make_pair(second_filter_column_value, first_filter_column_value);
  };

  return _generate_between(table, between_predicate_value, filter_column);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::generate_predicate_between_column_column(
    const std::shared_ptr<StoredTableNode>& table,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  const auto& between_predicate_column_functor =
      [](const std::shared_ptr<StoredTableNode>& table,
         const std::pair<std::string, CalibrationColumnSpecification>& filter_column)
      -> std::optional<std::pair<std::shared_ptr<AbstractExpression>, std::shared_ptr<AbstractExpression>>> {
    const auto filter_column_expression = lqp_column_(table->get_column(filter_column.first));

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

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::generate_predicate_column_value(
    const std::shared_ptr<StoredTableNode>& table,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  const auto& filter_column_value_functor =
      [](const std::shared_ptr<StoredTableNode>& table,
         const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
        return _generate_value_expression(filter_column.second);
      };

  return _generate_column_predicate(table, filter_column_value_functor, filter_column);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::generate_predicate_column_column(
    const std::shared_ptr<StoredTableNode>& table,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  const auto& filter_column_column_functor =
      [](const std::shared_ptr<StoredTableNode>& table,
         const std::pair<std::string, CalibrationColumnSpecification>& filter_column)
      -> std::shared_ptr<AbstractExpression> {
    static std::mt19937 engine((std::random_device()()));

    const auto filter_column_expression = lqp_column_(table->get_column(filter_column.first));

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

  return _generate_column_predicate(table, filter_column_column_functor, filter_column);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::generate_predicate_like(
    const std::shared_ptr<StoredTableNode>& table,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  if (filter_column.second.type != DataType::String) return {};

  const auto lhs = lqp_column_(table->get_column(filter_column.first));
  const auto rhs = _generate_value_expression(filter_column.second, true);

  if (!rhs) {
    return {};
  }

  return like_(lhs, rhs);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::generate_predicate_equi_on_strings(
    const std::shared_ptr<StoredTableNode>& table,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  // We just want Equi Scans on String columns for now
  if (filter_column.second.type != DataType::String) return {};

  static std::mt19937 engine((std::random_device()()));

  const auto lqp_column_reference = table->get_column(filter_column.first);
  const auto lhs = lqp_column_(table->get_column(filter_column.first));

  // TODO(Sven): Get one existing value from column and filter by that
  const auto stored_table = StorageManager::get().get_table(table->table_name);
  const auto column_id = lqp_column_reference.original_column_id();

  std::uniform_int_distribution<uint64_t> row_number_dist(0, stored_table->row_count() - 1);
  const auto rhs = stored_table->get_value<std::string>(column_id, row_number_dist(engine));

  return equals_(lhs, rhs);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::generate_predicate_or(
    const std::shared_ptr<StoredTableNode>& table,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  const auto lhs = generate_predicate_column_value(table, filter_column);
  const auto rhs = generate_predicate_column_value(table, filter_column);

  return or_(lhs, rhs);
}

/**
 * Helper function to generate less-than-equal predicate
 * @param table
 * @param predicate_generator
 * @param filter_column
 * @return
 */
const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::_generate_column_predicate(
    const std::shared_ptr<StoredTableNode>& table, const PredicateGeneratorFunctor& predicate_generator,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  const auto lqp_column_reference = table->get_column(filter_column.first);
  const auto lhs = lqp_column_(lqp_column_reference);
  const auto rhs = predicate_generator(table, filter_column);

  if (!rhs) return {};

  return less_than_equals_(lhs, rhs);
}

/**
     * Helper function to generate between predicate
     * @param table
     * @param predicate_generator
     * @param filter_column
     * @return
     */
const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicates::_generate_between(
    const std::shared_ptr<StoredTableNode>& table, const BetweenPredicateGeneratorFunctor& predicate_generator,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column) {
  const auto column = lqp_column_(table->get_column(filter_column.first));

  const auto& between_values = predicate_generator(table, filter_column);

  if (!between_values) return {};

  return between_(column, between_values->first, between_values->second);
}

const std::shared_ptr<ValueExpression> CalibrationQueryGeneratorPredicates::_generate_value_expression(
    const CalibrationColumnSpecification& column_definition, const bool trailing_like) {
  auto column_type = column_definition.type;
  static std::mt19937 engine((std::random_device()()));

  std::uniform_int_distribution<uint16_t> int_dist(0, column_definition.distinct_values - 1);
  std::uniform_real_distribution<> float_dist(0, 1);
  std::uniform_int_distribution<uint16_t> char_dist(0, 25);

  switch (column_type) {
    case DataType::Int:
    case DataType::Long:
      return value_(int_dist(engine));
    case DataType::String: {
      const auto character = std::string(1, 'A' + char_dist(engine));
      if (trailing_like) {
        return value_(character + '%');
      }
      return value_(character);
    }
    case DataType::Float:
    case DataType::Double:
      return value_(float_dist(engine));
    case DataType::Bool:
    case DataType::Null:
    default:
      Fail("Unsupported data type in CalibrationQueryGeneratorPredicates, found " + data_type_to_string.left.at(column_type));
  }
}

}  // namespace opossum
