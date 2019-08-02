#include "duplicate_elimination_rule.hpp"

#include <map>
#include <string>

#include "boost/functional/hash.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

std::string DuplicateEliminationRule::name() const { return "Duplicate Elimination Rule"; }

void DuplicateEliminationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  _original_replacement_pairs.clear();
  _remaining_stored_table_nodes.clear();
  _sub_plans.clear();

  _print_traversal(node);

  _find_sub_plan_duplicates_traversal(node);

  for (const auto& [original, replacement] : _original_replacement_pairs) {
    for (const auto& output : original->outputs()) {
      if(original->type != LQPNodeType::Validate && original->type != LQPNodeType::StoredTable){
        const auto& input_side = original->get_input_side(output);
        output->set_input(input_side, replacement);
      }
    }
  }

  _adapt_expressions_traversal(node);

}

void DuplicateEliminationRule::_print_traversal(const std::shared_ptr<AbstractLQPNode>& node) const {
  if(node){
    std::cout << node->description() << ", " << node << "\n";
    _print_traversal(node->left_input());
    _print_traversal(node->right_input());
  }
}

void DuplicateEliminationRule::_find_sub_plan_duplicates_traversal(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto left_node = node->left_input();
  const auto right_node = node->right_input();

  if (left_node) {
    _find_sub_plan_duplicates_traversal(left_node);
  }
  if (right_node) {
    _find_sub_plan_duplicates_traversal(right_node);
  }
  const auto duplicate_iter = std::find_if(_sub_plans.cbegin(), _sub_plans.cend(),
                                           [&node](const auto& sub_plan) { return *node == *sub_plan; });
  if (duplicate_iter == _sub_plans.end()) {
    _sub_plans.emplace_back(node);
    if (node->type == LQPNodeType::StoredTable) {
      _remaining_stored_table_nodes.emplace_back(node);
    }
  } else {
    _original_replacement_pairs.emplace_back(node, *duplicate_iter);
  }
}

void DuplicateEliminationRule::_adapt_expressions_traversal(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node) {
    expressions_adapt_to_different_lqp(node->node_expressions, _remaining_stored_table_nodes);
    _adapt_expressions_traversal(node->left_input());
    _adapt_expressions_traversal(node->right_input());
  }
}

}  // namespace opossum
