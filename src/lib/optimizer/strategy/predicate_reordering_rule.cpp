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

std::string PredicateReorderingRule::name() const { return "Predicate Reordering Rule"; }

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
  // Store original input and output
  auto input = predicates.back()->left_input();
  const auto outputs = predicates.front()->outputs();
  const auto input_sides = predicates.front()->get_input_sides();

  // Setup cardinality estimation cache so that the statistics of `input` (which might be a big plan) do not need to
  // be determined repeatedly. For this, we hijack the `guarantee_join_graph()`-guarantee and via it promise the
  // CardinalityEstimator that we will not change the LQP below the `input` node by marking it as a "vertex".
  // This allows the CardinalityEstimator to compute the statistics of `input` once, cache them and then re-use them.
  const auto caching_cardinality_estimator = cost_estimator->cardinality_estimator->new_instance();
  caching_cardinality_estimator->guarantee_join_graph(JoinGraph{{input}, {}});

  // Estimate the output cardinalities of each individual predicate on top of the input LQP, i.e., predicates are
  // estimated independently
  auto nodes_and_cardinalities = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, Cardinality>>{};
  nodes_and_cardinalities.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    predicate->set_left_input(input);
    nodes_and_cardinalities.emplace_back(predicate, caching_cardinality_estimator->estimate_cardinality(predicate));
  }

  // Untie predicates from LQP, so we can freely retie them
  for (auto& predicate : predicates) {
    lqp_remove_node(predicate);
  }

  // Sort in descending order
  std::sort(nodes_and_cardinalities.begin(), nodes_and_cardinalities.end(),
            [&](auto& left, auto& right) { return left.second > right.second; });

  // Ensure that nodes are chained correctly
  nodes_and_cardinalities.back().first->set_left_input(input);

  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], nodes_and_cardinalities.front().first);
  }

  for (size_t predicate_index = 0; predicate_index + 1 < nodes_and_cardinalities.size(); predicate_index++) {
    nodes_and_cardinalities[predicate_index].first->set_left_input(nodes_and_cardinalities[predicate_index + 1].first);
  }
}

}  // namespace opossum
