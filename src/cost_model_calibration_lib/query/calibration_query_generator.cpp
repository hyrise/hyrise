#include "calibration_query_generator.hpp"

#include <algorithm>
#include <random>

#include "../configuration/calibration_table_specification.hpp"
#include "calibration_query_generator_aggregates.hpp"
#include "calibration_query_generator_join.hpp"
#include "calibration_query_generator_predicate.hpp"
#include "calibration_query_generator_projection.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/validate_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

CalibrationQueryGenerator::CalibrationQueryGenerator(
    const std::vector<std::pair<std::string, size_t>>& tables,
    const std::vector<CalibrationColumnSpecification>& column_specifications,
    const CalibrationConfiguration& configuration)
    : _column_specifications(column_specifications), _configuration(configuration), _tables(tables) {}

/**
 * This function generates all TableScan permutations for TPCH-6
 */
const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::generate_tpch_6() {
  std::vector<std::shared_ptr<AbstractLQPNode>> queries;

  const auto lineitem = StoredTableNode::make("lineitem");

  const auto shipdate_column = lineitem->get_column("l_shipdate");
  const auto discount_column = lineitem->get_column("l_discount");
  const auto quantity_column = lineitem->get_column("l_quantity");

  const auto shipdate_gte =
      PredicateNode::make(greater_than_equals_(lqp_column_(shipdate_column), value_("1994-01-01")));
  const auto shipdate_lt = PredicateNode::make(less_than_(lqp_column_(shipdate_column), value_("1995-01-01")));
  const auto discount = PredicateNode::make(between_(lqp_column_(discount_column), value_(0.05), value_(0.07001)));
  const auto quantity = PredicateNode::make(less_than_(lqp_column_(quantity_column), value_(24)));

  std::vector<std::vector<std::shared_ptr<AbstractLQPNode>>> predicate_node_permutations = {
      {shipdate_gte, shipdate_lt, discount, quantity}, {shipdate_gte, shipdate_lt, quantity, discount},
      {shipdate_gte, discount, shipdate_lt, quantity}, {shipdate_gte, discount, quantity, shipdate_lt},
      {shipdate_gte, quantity, shipdate_lt, discount}, {shipdate_gte, quantity, discount, shipdate_lt},
      {quantity, shipdate_gte, discount, shipdate_lt}, {quantity, shipdate_gte, shipdate_lt, discount},
      {quantity, shipdate_lt, shipdate_gte, discount}, {quantity, shipdate_lt, discount, shipdate_gte},
      {quantity, discount, shipdate_lt, shipdate_gte}, {quantity, discount, shipdate_gte, shipdate_lt},
      {discount, quantity, shipdate_gte, shipdate_lt}, {discount, quantity, shipdate_lt, shipdate_gte},
      {discount, shipdate_lt, quantity, shipdate_gte}, {discount, shipdate_lt, shipdate_gte, quantity},
      {discount, shipdate_gte, shipdate_lt, quantity}, {discount, shipdate_gte, quantity, shipdate_lt},
      {shipdate_lt, discount, shipdate_gte, quantity}, {shipdate_lt, discount, quantity, shipdate_gte},
      {shipdate_lt, quantity, discount, shipdate_gte}, {shipdate_lt, quantity, shipdate_gte, discount},
      {shipdate_lt, shipdate_gte, discount, quantity}, {shipdate_lt, shipdate_gte, quantity, discount}};

  for (const auto& permutation : predicate_node_permutations) {
    std::shared_ptr<AbstractLQPNode> previous_node = lineitem;
    for (const auto& node : permutation) {
      const auto copied_node = node->deep_copy({{lineitem, lineitem}});
      copied_node->set_left_input(previous_node);
      previous_node = copied_node;
    }

    queries.push_back(previous_node);
  }

  return queries;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::generate_queries() const {
  std::vector<std::shared_ptr<AbstractLQPNode>> queries;

  const auto& add_queries_if_present = [&](std::vector<std::shared_ptr<AbstractLQPNode>>& existing_queries,
                                           const std::vector<std::shared_ptr<AbstractLQPNode>>& new_queries) {
    for (const auto& query : new_queries) {
      if (query) {
        existing_queries.push_back(query);
      }
    }
  };

  //  for (const auto& table_name : _table_names) {
  //    add_queries_if_present(queries, _generate_aggregate(table_name));
  //  }

  // TODO(Sven): Permutations produce unnecessarily duplicate scans, e.g., when running a single column scan,
  // but with varying permutations for the second and third column
  const auto permutations = CalibrationQueryGeneratorPredicate::generate_predicate_permutations(_tables, _configuration);
  // TODO(Sven): shuffle permutations to cover a wide range of queries faster
  for (const auto& permutation : permutations) {
    add_queries_if_present(
        queries,
        _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_column_value));

    add_queries_if_present(
        queries,
        _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_column_column));

    add_queries_if_present(
        queries,
        _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value));

//    add_queries_if_present(
//        queries, _generate_table_scan(permutation,
//                                      CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column));

    if (permutation.data_type == DataType::String) {
      add_queries_if_present(
              queries, _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_like));

      add_queries_if_present(
              queries,
              _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings));
    }

    add_queries_if_present(
        queries, _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_or));
  }

//  const auto join_permutations = CalibrationQueryGeneratorJoin::generate_join_permutations(_tables, _configuration);
//  for (const auto& permutation : join_permutations) {
//    const auto& join_queries = _generate_join(permutation);
//    add_queries_if_present(queries, join_queries);
//  }

  std::cout << "Generated " << queries.size() << " queries." << std::endl;

  return queries;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_table_scan(
    const CalibrationQueryGeneratorPredicateConfiguration& configuration,
    const PredicateGeneratorFunctor& predicate_generator) const {
  const auto table = StoredTableNode::make(configuration.table_name);

  const auto predicates = CalibrationQueryGeneratorPredicate::generate_predicates(
      predicate_generator, _column_specifications, table, configuration);

  // TODO(Sven): Add test
  if (predicates.empty()) {
    return {};
  }

  std::vector<std::shared_ptr<AbstractLQPNode>> output;

  // Use additional ValidateNode to force a reference-segment TableScan
  for (const auto& predicate_node : predicates) {
    if (configuration.reference_column) {
      const auto validate_node = ValidateNode::make();
      validate_node->set_left_input(table);
      predicate_node->set_left_input(validate_node);
    } else {
      predicate_node->set_left_input(table);
    }

    output.push_back(predicate_node);
  }

  return output;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_join(
    const CalibrationQueryGeneratorJoinConfiguration& configuration) const {
  const auto left_table = StoredTableNode::make(configuration.left_table_name);
  const auto right_table = StoredTableNode::make(configuration.right_table_name);

  const auto join_nodes =
      CalibrationQueryGeneratorJoin{configuration, _column_specifications}.generate_join(left_table, right_table);

  if (join_nodes.empty()) {
    return {};
  }

  for (const auto& join_node : join_nodes) {
    if (configuration.reference_column) {
      const auto left_validate_node = ValidateNode::make();
      const auto right_validate_node = ValidateNode::make();

      left_validate_node->set_left_input(left_table);
      right_validate_node->set_left_input(right_table);

      join_node->set_left_input(left_validate_node);
      join_node->set_right_input(right_validate_node);

    } else {
      join_node->set_left_input(left_table);
      join_node->set_right_input(right_table);
    }
  }

  return join_nodes;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_aggregate(
    const std::string& table_name) const {
  const auto table = StoredTableNode::make(table_name);

  const auto aggregate_node = CalibrationQueryGeneratorAggregate::generate_aggregates();

  aggregate_node->set_left_input(table);
  return {aggregate_node};
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_projection(
    const std::vector<LQPColumnReference>& columns) const {
  return {CalibrationQueryGeneratorProjection::generate_projection(columns)};
}
}  // namespace opossum
