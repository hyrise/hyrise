#include "cardinality_writer.hpp"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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
  // We can guarantee the LQP never changes during visualization and thus avoid redundant estimations for subplans
  _cardinality_estimator.guarantee_bottom_up_construction();
}

void CardinalityWriter::write_cardinalities(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots,
																						const std::vector<std::shared_ptr<AbstractOperator>>& pqp_roots,
																						const std::string& benchmark_item_id){
    _build_graph(lqp_roots);
		_build_pqp_graph(pqp_roots);

		//Assert(_lqp_nodes.size() != _pqp_nodes.size(), "LQP and PQP have differing number of nodes / differing structure. Can't match.");

		// std::cout << "LQP SIZE " << _lqp_nodes.size() << std::endl;
		// std::cout << "PQP SIZE " << _pqp_nodes.size() << std::endl;

		const auto lqp_nodes_size = _lqp_nodes.size();
		const auto pqp_nodes_size = _pqp_nodes.size();
		
		const auto number_nodes = std::min(_lqp_nodes.size(), _pqp_nodes.size());

		for (auto node = 0u; node < number_nodes; node++) {

			const auto lqp_description = _lqp_nodes[node]->description();
			const auto pqp_description = _pqp_nodes[node]->description();
			// std::cout << _lqp_nodes[node]->description() << std::endl;
			// std::cout << _pqp_nodes[node]->name() << " " << _pqp_nodes[node]->description() << std::endl;
			
			const auto lqp_row_count = _cardinality_estimator.estimate_cardinality(_lqp_nodes[node]);
			
			auto pqp_row_count = NAN;
			const auto& performance_data = *_pqp_nodes[node]->performance_data;

  		if (_pqp_nodes[node]->executed() && performance_data.has_output) {
    		pqp_row_count = performance_data.output_row_count;
  		}

			// std::cout << "LQP Rowcount: " << lqp_row_count << std::endl;
			// std::cout << "PQP Rowcount: " << pqp_row_count << std::endl;

  		std::ofstream output_file;
  		output_file.open ("./benchmark_cardinality_estimation.csv", std::ios_base::app);
  		output_file << benchmark_item_id << "," << lqp_description << "," << pqp_description << "," << lqp_row_count << "," << pqp_row_count << "\n";
  		output_file.close();

		}

		// Print debug information about possible different numbers of lqp and pqp nodes
		std::ofstream output_file;
		output_file.open ("./benchmark_cardinality_estimation_debug_info.csv", std::ios_base::app);
		output_file << benchmark_item_id << "," << lqp_nodes_size << "," << pqp_nodes_size << "\n";
		output_file.close();

}

void CardinalityWriter::_build_graph(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots) {
  std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visualized_nodes;
  ExpressionUnorderedSet visualized_sub_queries;

  for (const auto& root : lqp_roots) {
    _build_subtree(root, visualized_nodes, visualized_sub_queries);
  }
}

void CardinalityWriter::_build_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                   std::unordered_set<std::shared_ptr<const AbstractLQPNode>>& visualized_nodes,
                                   ExpressionUnorderedSet& visualized_sub_queries) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped Nodes
  if (visualized_nodes.find(node) != visualized_nodes.end()) return;
  visualized_nodes.insert(node);
	_lqp_nodes.push_back(node);

  auto node_label = node->description();
  if (!node->comment.empty()) {
    node_label += "\\n(" + node->comment + ")";
  }

  //std::cout << "LQP: " << node_label << std::endl;

  if (node->left_input()) {
    auto left_input = node->left_input();
    _build_subtree(left_input, visualized_nodes, visualized_sub_queries);
  }

  if (node->right_input()) {
    auto right_input = node->right_input();
    _build_subtree(right_input, visualized_nodes, visualized_sub_queries);
  }

  // Visualize subqueries
  for (const auto& expression : node->node_expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (!subquery_expression) return ExpressionVisitation::VisitArguments;

      if (!visualized_sub_queries.emplace(subquery_expression).second) return ExpressionVisitation::VisitArguments;

      _build_subtree(subquery_expression->lqp, visualized_nodes, visualized_sub_queries);

      return ExpressionVisitation::VisitArguments;
    });
  }
}

void CardinalityWriter::_build_pqp_graph(const std::vector<std::shared_ptr<AbstractOperator>>& plans) {
  std::unordered_set<std::shared_ptr<const AbstractOperator>> visualized_ops;

  for (const auto& plan : plans) {
    _build_pqp_subtree(plan, visualized_ops);
  }
}

void CardinalityWriter::_build_pqp_subtree(const std::shared_ptr<const AbstractOperator>& op,
                                   std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped PQPs
  if (visualized_ops.find(op) != visualized_ops.end()) return;
  visualized_ops.insert(op);
	_pqp_nodes.push_back(op);

	auto op_label = op->name();
  if (!op->description().empty()) {
    op_label += "\\n(" + op->description() + ")";
  }

  //std::cout << "PQP: " << op_label << std::endl;

  if (op->left_input()) {
    auto left = op->left_input();
    _build_pqp_subtree(left, visualized_ops);
  }

  if (op->right_input()) {
    auto right = op->right_input();
    _build_pqp_subtree(right, visualized_ops);
  }

  switch (op->type()) {
    case OperatorType::Projection: {
      const auto projection = std::dynamic_pointer_cast<const Projection>(op);
      for (const auto& expression : projection->expressions) {
        _visualize_pqp_subqueries(op, expression, visualized_ops);
      }
    } break;

    case OperatorType::TableScan: {
      const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
      _visualize_pqp_subqueries(op, table_scan->predicate(), visualized_ops);
    } break;

    case OperatorType::Limit: {
      const auto limit = std::dynamic_pointer_cast<const Limit>(op);
      _visualize_pqp_subqueries(op, limit->row_count_expression(), visualized_ops);
    } break;

    default: {
    }  // OperatorType has no expressions
  }
}

void CardinalityWriter::_visualize_pqp_subqueries(const std::shared_ptr<const AbstractOperator>& op,
                                          const std::shared_ptr<AbstractExpression>& expression,
                                          std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops) {
  visit_expression(expression, [&](const auto& sub_expression) {
    const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
    if (!pqp_subquery_expression) return ExpressionVisitation::VisitArguments;

    _build_pqp_subtree(pqp_subquery_expression->pqp, visualized_ops);

    return ExpressionVisitation::VisitArguments;
  });
}

}  // namespace opossum
