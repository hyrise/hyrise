#include "predicate_reordering_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string PredicateReorderingRule::name() const { return "Predicate Reordering Rule"; }

bool PredicateReorderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto reordered = false;

  if (node->type == LQPNodeType::Predicate) {
    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type == LQPNodeType::Predicate) {
      // Once a node has multiple outputs, we're not talking about a Predicate chain anymore
      if (current_node->outputs().size() > 1) {
        break;
      }

      predicate_nodes.emplace_back(std::dynamic_pointer_cast<PredicateNode>(current_node));
      current_node = current_node->left_input();
    }

    /**
     * A chain of predicates was found.
     * Sort PredicateNodes in descending order with regards to the expected row_count
     * Continue rule in deepest input
     */
    if (predicate_nodes.size() > 1) {
      reordered = _reorder_predicates(predicate_nodes);
      reordered |= _apply_to_inputs(predicate_nodes.back());
    } else {
      // No chain was found, continue with the current nodes inputren.
      reordered = _apply_to_inputs(node);
    }
  } else {
    reordered = _apply_to_inputs(node);
  }

  return reordered;
}

bool PredicateReorderingRule::_reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const {
  // Store original input and output
  auto input = predicates.back()->left_input();
  const auto outputs = predicates.front()->outputs();
  const auto input_sides = predicates.front()->get_input_sides();

  const auto sort_predicate = [&](auto& left, auto& right) {
    return left->derive_statistics_from(input)->row_count() > right->derive_statistics_from(input)->row_count();
  };

  if (std::is_sorted(predicates.begin(), predicates.end(), sort_predicate)) {
    return false;
  }

  // Untie predicates from LQP, so we can freely retie them
  for (auto& predicate : predicates) {
    lqp_remove_node(predicate);
  }

  // Sort in descending order
  std::sort(predicates.begin(), predicates.end(), sort_predicate);

  // Ensure that nodes are chained correctly
  predicates.back()->set_left_input(input);

  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], predicates.front());
  }

  for (size_t predicate_index = 0; predicate_index < predicates.size() - 1; predicate_index++) {
    predicates[predicate_index]->set_left_input(predicates[predicate_index + 1]);
  }

  return true;
}

}  // namespace opossum
