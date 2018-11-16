#include "calibration_query_generator.hpp"

#include <random>

#include "../configuration/calibration_table_specification.hpp"
#include "calibration_query_generator_aggregates.hpp"
#include "calibration_query_generator_join.hpp"
#include "calibration_query_generator_predicate.hpp"
#include "calibration_query_generator_projection.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

CalibrationQueryGenerator::CalibrationQueryGenerator(
    const std::vector<CalibrationColumnSpecification>& column_specifications)
    : _column_specifications(column_specifications){};

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::generate_queries() const {
  std::vector<std::shared_ptr<AbstractLQPNode>> queries;

  const auto& add_query_if_present = [](std::vector<std::shared_ptr<AbstractLQPNode>>& vector,
                                        const std::shared_ptr<AbstractLQPNode>& query) {
    if (query) {
      vector.push_back(query);
    }
  };

  add_query_if_present(queries, CalibrationQueryGenerator::_generate_aggregate());

  add_query_if_present(queries,
                       _generate_table_scan(CalibrationQueryGeneratorPredicate::generate_predicate_column_value));

  add_query_if_present(queries,
                       _generate_table_scan(CalibrationQueryGeneratorPredicate::generate_predicate_column_column));

  add_query_if_present(
      queries, _generate_table_scan(CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value));

  add_query_if_present(
      queries, _generate_table_scan(CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column));

  add_query_if_present(queries, _generate_table_scan(CalibrationQueryGeneratorPredicate::generate_predicate_like));

  add_query_if_present(queries, _generate_table_scan(CalibrationQueryGeneratorPredicate::generate_predicate_or));

  add_query_if_present(queries,
                       _generate_table_scan(CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings));

  // Generates the same query using all available JoinTypes
  const auto join_queries = _generate_join();
  for (const auto& query : join_queries) {
    add_query_if_present(queries, query);
  }

  return queries;
}

const std::shared_ptr<MockNode> CalibrationQueryGenerator::_column_specification_to_mock_node() const {
  std::vector<std::pair<DataType, std::string>> column_definitions;
  for (const auto& column : _column_specifications) {
    column_definitions.emplace_back(column.type, column.column_name);
  }

  return MockNode::make(column_definitions);
}

const std::shared_ptr<AbstractLQPNode> CalibrationQueryGenerator::_generate_table_scan(
    const PredicateGeneratorFunctor& predicate_generator) const {
  const auto table = _column_specification_to_mock_node();

  CalibrationQueryGeneratorPredicateConfiguration configuration{EncodingType::Unencoded, DataType::String, 0.1f, false};

  auto predicate = CalibrationQueryGeneratorPredicate::generate_predicates(predicate_generator, _column_specifications,
                                                                           table, 3, configuration);

  return predicate;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_join() const {
  static std::mt19937 engine((std::random_device()()));

  const auto left_table = _column_specification_to_mock_node();
  const auto right_table = _column_specification_to_mock_node();

  const auto join_nodes = CalibrationQueryGeneratorJoin::generate_join(
      CalibrationQueryGeneratorJoin::generate_join_predicate, left_table, right_table);

  if (join_nodes.empty()) {
    return {};
  }

  CalibrationQueryGeneratorPredicateConfiguration configuration{EncodingType::Unencoded, DataType::String, 0.1f, false};

  auto left_predicate = CalibrationQueryGeneratorPredicate::generate_predicates(
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value, _column_specifications, left_table, 2,
      configuration);

  auto right_predicate = CalibrationQueryGeneratorPredicate::generate_predicates(
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value, _column_specifications, right_table, 2,
      configuration);

  // We are not interested in queries without Predicates
  if (!left_predicate || !right_predicate) {
    return {};
  }

  for (const auto& join_node : join_nodes) {
    join_node->set_left_input(left_predicate);
    join_node->set_right_input(right_predicate);
  }

  return join_nodes;
}

const std::shared_ptr<AbstractLQPNode> CalibrationQueryGenerator::_generate_aggregate() const {
  const auto table = _column_specification_to_mock_node();

  const auto aggregate_node = CalibrationQueryGeneratorAggregate::generate_aggregates();

  CalibrationQueryGeneratorPredicateConfiguration configuration{EncodingType::Unencoded, DataType::String, 0.1f, false};

  auto predicate = CalibrationQueryGeneratorPredicate::generate_predicates(
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value, _column_specifications, table, 1,
      configuration);

  if (!predicate) {
    std::cout << "Failed to generate predicate for Aggregate" << std::endl;
    return {};
  }

  aggregate_node->set_left_input(predicate);
  return aggregate_node;
}

const std::shared_ptr<ProjectionNode> CalibrationQueryGenerator::_generate_projection(
    const std::vector<LQPColumnReference>& columns) const {
  return CalibrationQueryGeneratorProjection::generate_projection(columns);
}
}  // namespace opossum
