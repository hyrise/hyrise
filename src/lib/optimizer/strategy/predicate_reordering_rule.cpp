#include "predicate_reordering_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "cost_estimation/abstract_cost_estimator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

void PredicateReorderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  DebugAssert(cost_estimator, "PredicateReorderingRule requires cost estimator to be set");

  // Validate can be seen as a Predicate on the MVCC column
  if (node->type == LQPNodeType::Predicate || node->type == LQPNodeType::Validate) {
    std::vector<std::shared_ptr<AbstractLQPNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type == LQPNodeType::Predicate || current_node->type == LQPNodeType::Validate) {
      // Once a node has multiple outputs, we're not talking about a Predicate chain anymore
      if (current_node->outputs().size() > 1) {
        break;
      }

      predicate_nodes.emplace_back(current_node);
      current_node = current_node->left_input();
    }

    /**
     * A chain of predicates was found.
     * Sort PredicateNodes in descending order with regards to the expected row_count
     * Continue rule in deepest input
     */
    if (predicate_nodes.size() > 1) {
      const auto input = predicate_nodes.back()->left_input();
      _reorder_predicates(predicate_nodes);
      apply_to(input);
    }
  }

  _apply_to_inputs(node);
}

void PredicateReorderingRule::_reorder_predicates(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& predicates) const {
  // Workaround for the now const'ed parameter
  std::vector<std::shared_ptr<AbstractLQPNode>> non_const_predicates(predicates.begin(), predicates.end());

  // Store original input and output
  auto input = non_const_predicates.back()->left_input();
  const auto outputs = non_const_predicates.front()->outputs();
  const auto input_sides = non_const_predicates.front()->get_input_sides();

  Cost minimal_cost{std::numeric_limits<float>::max()};
  auto minimal_order = non_const_predicates;

  // std::cout << "Before" << std::endl;
  // non_const_predicates.front()->print();

  // Untie predicates from LQP, so we can freely retie them
  for (auto& predicate : non_const_predicates) {
    lqp_remove_node(predicate);
  }

  do {
    // Ensure that nodes are chained correctly
    non_const_predicates.back()->set_left_input(input);

    for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
      outputs[output_idx]->set_input(input_sides[output_idx], non_const_predicates.front());
    }

    for (size_t predicate_index = 0; predicate_index < non_const_predicates.size() - 1; predicate_index++) {
      predicates[predicate_index]->set_left_input(non_const_predicates[predicate_index + 1]);
    }

    //        non_const_predicates.front()->print();
    const auto estimated_cost = _cost_estimator->estimate_plan_cost(non_const_predicates.front());
    //        std::cout << "Estimated cost: " << estimated_cost << std::endl;

    if (estimated_cost < minimal_cost) {
      minimal_cost = estimated_cost;
      minimal_order = non_const_predicates;
    }

    // Untie non_const_predicates from LQP, so we can freely retie them
    for (auto& predicate : non_const_predicates) {
      lqp_remove_node(predicate);
    }
  } while (std::prev_permutation(non_const_predicates.begin(), non_const_predicates.end()));

  // Ensure that nodes are chained correctly
  minimal_order.back()->set_left_input(input);

  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], minimal_order.front());
  }

  for (size_t predicate_index = 0; predicate_index < minimal_order.size() - 1; predicate_index++) {
    minimal_order[predicate_index]->set_left_input(minimal_order[predicate_index + 1]);
  }

  // std::cout << "Foobar" << std::endl;
  // minimal_order.front()->print();
}

}  // namespace opossum
