#include "cardinality_writer.hpp"

#include <fstream>
#include <iostream>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"

namespace opossum {

CardinalityWriter::CardinalityWriter() {
  // We can guarantee the LQP never changes during visits and thus avoid redundant estimations for subplans
  _cardinality_estimator.guarantee_bottom_up_construction();
}

void CardinalityWriter::write_cardinalities(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots,
                                            const std::vector<std::shared_ptr<AbstractOperator>>& pqp_roots,
                                            const std::string& benchmark_item_id) {
  // Traverse LQP and PQP trees and save every node along the way in _lqp_nodes and _pqp_nodes
  _build_lqp_graph(lqp_roots);
  _build_pqp_graph(pqp_roots);

  const auto lqp_nodes_size = _lqp_nodes.size();
  const auto pqp_nodes_size = _pqp_nodes.size();

  // We assume that when we traverse LQP and PQP the same way, we will get the same nodes.
  // All nodes that don't match up are ignored. This works well for the majority of tested LQPs and PQPs.
  const auto number_nodes = std::min(lqp_nodes_size, pqp_nodes_size);

  // APPEND mode is necessary, since the method will be called multiple times during benchmark execution
  // and the previous results shouldn't be erased.
  std::ofstream output_estimations;
  output_estimations.open("./benchmark_cardinality_estimation.csv", std::ios_base::app);

  // Open debug CSV file for PQP and LQP node number information, which can be used to identify possible
  // benchmark items with mismatched nodes and for manual correction of the matching.
  std::ofstream output_debug;
  output_debug.open("./benchmark_cardinality_estimation_mismatch_info.csv", std::ios_base::app);

  // If files are empty, write CSV headers.
  // Use semicolon as seperator instead of comma to be able to work with database values containing commas
  const auto *const seperator = ";";
  std::ifstream csv_file;
  csv_file.open("./benchmark_cardinality_estimation.csv");
  if (csv_file.peek() == std::ifstream::traits_type::eof()) {
    output_estimations << "Benchmark Item ID"
                       << seperator
                       << "LQP Node"
                       << seperator
                       << "PQP Node"
                       << seperator
                       << "LQP Row count"
                       << seperator
                       << "PQP Row count"
                       << "\n";
  }
  csv_file.close();

  csv_file.open("./benchmark_cardinality_estimation_mismatch_info.csv");
  if (csv_file.peek() == std::ifstream::traits_type::eof()) {
    output_debug << "Benchmark Item ID"
                 << seperator
                 << "LQP Number of nodes"
                 << seperator
                 << "PQP Number of nodes"
                 << "\n";
  }
  csv_file.close();

  for (auto node = 0u; node < number_nodes; node++) {
    // Get estimated and actual output cardinalities
    const auto lqp_row_count = _cardinality_estimator.estimate_cardinality(_lqp_nodes[node]);

    auto pqp_row_count = NAN;
    const auto& performance_data = *_pqp_nodes[node]->performance_data;

    if (_pqp_nodes[node]->executed() && performance_data.has_output) {
      pqp_row_count = static_cast<float>(performance_data.output_row_count);
    }

    // Write benchmark id, node descriptions and cardinalities in CSV
    output_estimations << benchmark_item_id << seperator << _lqp_nodes[node]->description() << seperator
                       << _pqp_nodes[node]->description() << seperator << lqp_row_count
                       << seperator << pqp_row_count << "\n";
  }

  // Write debug information about possible different numbers of lqp and pqp nodes
  output_debug << benchmark_item_id << seperator << lqp_nodes_size << seperator << pqp_nodes_size << "\n";

  output_estimations.close();
  output_debug.close();
}

void CardinalityWriter::_build_lqp_graph(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots) {
  std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visited_nodes;
  ExpressionUnorderedSet visited_sub_queries;

  for (const auto& root : lqp_roots) {
    _build_lqp_subtree(root, visited_nodes, visited_sub_queries);
  }
}

void CardinalityWriter::_build_lqp_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                           std::unordered_set<std::shared_ptr<const AbstractLQPNode>>& visited_nodes,
                                           ExpressionUnorderedSet& visited_sub_queries) {
  if (visited_nodes.find(node) != visited_nodes.end()) return;
  visited_nodes.insert(node);

  _lqp_nodes.push_back(node);

  if (node->left_input()) {
    const auto left_input = node->left_input();
    _build_lqp_subtree(left_input, visited_nodes, visited_sub_queries);
  }

  if (node->right_input()) {
    const auto right_input = node->right_input();
    _build_lqp_subtree(right_input, visited_nodes, visited_sub_queries);
  }

  // Traverse subqueries
  for (const auto& expression : node->node_expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (!subquery_expression) return ExpressionVisitation::VisitArguments;

      if (!visited_sub_queries.emplace(subquery_expression).second) return ExpressionVisitation::VisitArguments;

      _build_lqp_subtree(subquery_expression->lqp, visited_nodes, visited_sub_queries);

      return ExpressionVisitation::VisitArguments;
    });
  }
}

void CardinalityWriter::_build_pqp_graph(const std::vector<std::shared_ptr<AbstractOperator>>& pqp_roots) {
  std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_ops;

  for (const auto& plan : pqp_roots) {
    _build_pqp_subtree(plan, visited_ops);
  }
}

void CardinalityWriter::_build_pqp_subtree(const std::shared_ptr<const AbstractOperator>& op,
                                           std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_ops) {
  if (visited_ops.find(op) != visited_ops.end()) return;
  visited_ops.insert(op);

  _pqp_nodes.push_back(op);

  if (op->left_input()) {
    const auto left = op->left_input();
    _build_pqp_subtree(left, visited_ops);
  }

  if (op->right_input()) {
    const auto right = op->right_input();
    _build_pqp_subtree(right, visited_ops);
  }

  switch (op->type()) {
    case OperatorType::Projection: {
      const auto projection = std::dynamic_pointer_cast<const Projection>(op);
      for (const auto& expression : projection->expressions) {
        _visit_pqp_subqueries(op, expression, visited_ops);
      }
    } break;

    case OperatorType::TableScan: {
      const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
      _visit_pqp_subqueries(op, table_scan->predicate(), visited_ops);
    } break;

    case OperatorType::Limit: {
      const auto limit = std::dynamic_pointer_cast<const Limit>(op);
      _visit_pqp_subqueries(op, limit->row_count_expression(), visited_ops);
    } break;

    default: {
    }  // OperatorType has no expressions
  }
}

void CardinalityWriter::_visit_pqp_subqueries(
    const std::shared_ptr<const AbstractOperator>& op, const std::shared_ptr<AbstractExpression>& expression,
    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_ops) {
  visit_expression(expression, [&](const auto& sub_expression) {
    const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
    if (!pqp_subquery_expression) return ExpressionVisitation::VisitArguments;

    _build_pqp_subtree(pqp_subquery_expression->pqp, visited_ops);

    return ExpressionVisitation::VisitArguments;
  });
}

}  // namespace opossum
