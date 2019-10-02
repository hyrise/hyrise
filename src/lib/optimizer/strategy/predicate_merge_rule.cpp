#include "predicate_merge_rule.hpp"

#include "expression/expression_functional.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

void PredicateMergeRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "PredicateMergeRule needs root to hold onto");

  std::queue<std::shared_ptr<AbstractLQPNode>> mergeable_nodes;
  size_t union_node_count = 0;
  visit_lqp(root, [&](const auto& node) {
    if (node->type == LQPNodeType::Union || node->type == LQPNodeType::Predicate) {
      mergeable_nodes.push(node);
    }
    if (node->type == LQPNodeType::Union) {
      union_node_count++;
    }
    return LQPVisitation::VisitInputs;
  });


  if (union_node_count < optimization_threshold) {
    return;
  }

  while (!mergeable_nodes.empty()) {
    const auto node = mergeable_nodes.front();
    mergeable_nodes.pop();

    if (node->output_count() == 0) {
      // The node was already removed.
      continue;
    }

    switch (node->type) {
      case LQPNodeType::Predicate:
        _merge_conjunction(std::static_pointer_cast<PredicateNode>(node));
        break;

      case LQPNodeType::Union:
        _merge_disjunction(std::static_pointer_cast<UnionNode>(node));
        break;

      default: {}
    }
  };
}

void PredicateMergeRule::_merge_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const {
  std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

  // Build predicate chain
  std::shared_ptr<AbstractLQPNode> current_node = predicate_node;
  while (current_node->type == LQPNodeType::Predicate) {
    // Once a node has multiple outputs, we're not talking about a predicate chain anymore. However, a new chain can
    // start here.
    if (current_node->outputs().size() > 1 && !predicate_nodes.empty()) {
      break;
    }

    predicate_nodes.emplace_back(std::static_pointer_cast<PredicateNode>(current_node));
    current_node = current_node->left_input();
  }

  // Merge predicate chain
  if (predicate_nodes.size() > 1) {
    auto merged_predicate = predicate_nodes.front()->predicate();
    for (auto predicate_idx = size_t{1}; predicate_idx < predicate_nodes.size(); ++predicate_idx) {
      const auto current_predicate_node = predicate_nodes[predicate_idx];
      merged_predicate = and_(current_predicate_node->predicate(), merged_predicate);
      lqp_remove_node(current_predicate_node);
    }

    const auto merged_predicate_node = PredicateNode::make(merged_predicate);
    lqp_replace_node(predicate_nodes.front(), merged_predicate_node);

    const auto parent_union_node = std::dynamic_pointer_cast<UnionNode>(merged_predicate_node->outputs().front());
    if (parent_union_node) _merge_disjunction(parent_union_node);
  }
}

void PredicateMergeRule::_merge_disjunction(const std::shared_ptr<UnionNode>& union_node) const {
  const auto left_node = std::dynamic_pointer_cast<PredicateNode>(union_node->left_input());
  const auto right_node = std::dynamic_pointer_cast<PredicateNode>(union_node->right_input());
  if (left_node && right_node &&
      left_node->left_input() == right_node->left_input() &&
      left_node->output_count() == 1 && right_node->output_count() == 1) {
    // Merge diamond
    const auto merged_predicate = or_(left_node->predicate(), right_node->predicate());

    const auto merged_predicate_node = PredicateNode::make(merged_predicate);
    lqp_remove_node(left_node);
    lqp_remove_node(right_node);
    union_node->set_right_input(nullptr);
    lqp_replace_node(union_node, merged_predicate_node);

    const auto parent_union_node = std::dynamic_pointer_cast<UnionNode>(merged_predicate_node->outputs().front());
    if (parent_union_node) _merge_disjunction(parent_union_node);

    if (merged_predicate_node->output_count()) {
      // There was no disjunction above that could be merged
      const auto parent_predicate_node = std::dynamic_pointer_cast<PredicateNode>(merged_predicate_node->outputs().front());
      if (parent_predicate_node) {
        _merge_conjunction(parent_predicate_node);
      } else {
        _merge_conjunction(merged_predicate_node);
      }
    }
  }
}

}  // namespace opossum
