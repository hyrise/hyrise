#include "predicate_reordering_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace {
using namespace opossum;  // NOLINT

// Returns whether a certain node is a "predicate-style" node, i.e., a node that can be moved freely within a predicate
// chain.
bool is_predicate_style_node(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type == LQPNodeType::Predicate) return true;

  // Validate can be seen as a Predicate on the MVCC column
  if (node->type == LQPNodeType::Validate) return true;

  // Semi-/Anti-Joins also reduce the number of tuples and can be freely reordered within a chain of predicates. This
  // might place the join below a validate node, but since it is not a "proper" join (i.e., one that returns columns
  // from multiple tables), the validate will stil be able to operate on the semi join's output.
  if (node->type == LQPNodeType::Join) {
    const auto& join_node = static_cast<JoinNode&>(*node);
    if (join_node.join_mode == JoinMode::Semi || join_node.join_mode == JoinMode::AntiNullAsTrue ||
        join_node.join_mode == JoinMode::AntiNullAsFalse) {
      return true;
    }
  }

  return false;
}
}  // namespace

namespace opossum {

void PredicateReorderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  DebugAssert(cost_estimator, "PredicateReorderingRule requires cost estimator to be set");
  Assert(root->type == LQPNodeType::Root, "PredicateReorderingRule needs root to hold onto");

  visit_lqp(root, [&](const auto& node) {
    if (is_predicate_style_node(node)) {
      std::vector<std::shared_ptr<AbstractLQPNode>> predicate_nodes;

      // Gather adjacent PredicateNodes
      auto current_node = node;
      while (is_predicate_style_node(current_node)) {
        // Once a node has multiple outputs, we're not talking about a predicate chain anymore. However, a new chain can
        // start here.
        if (current_node->outputs().size() > 1 && !predicate_nodes.empty()) {
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
        _reorder_predicates(predicate_nodes);
      }
    }

    return LQPVisitation::VisitInputs;
  });
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
  for (const auto& predicate : predicates) {
    lqp_remove_node(predicate, AllowRightInput::Yes);
  }

  // Sort in descending order
  std::sort(nodes_and_cardinalities.begin(), nodes_and_cardinalities.end(),
            [&](auto& left, auto& right) { return left.second > right.second; });

  auto nacs = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, Cardinality>>{};
  std::copy_if(nodes_and_cardinalities.begin(), nodes_and_cardinalities.end(), std::back_inserter(nacs), [&](auto& nac) {
    const auto node = nac.first;
    if (node->type != LQPNodeType::Predicate) return true;
    const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
    const auto predicate = predicate_node->predicate();
    if (const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(predicate)) {
      const auto& column = std::dynamic_pointer_cast<LQPColumnExpression>(is_null_expression->operand());
      if (!column) return true;

      const auto& stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column->original_node.lock());
      if (!stored_table_node) return true;

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      const auto original_column_id = column->original_column_id;
      const auto table_column_definition = table->column_definitions()[original_column_id];
      if (table_column_definition.nullable == false) return false;
    }
    return true;
  });

  // Ensure that nodes are chained correctly
  nacs.back().first->set_left_input(input);

  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], nacs.front().first);
  }

  for (size_t predicate_index = 0; predicate_index + 1 < nacs.size(); predicate_index++) {
    nacs[predicate_index].first->set_left_input(nacs[predicate_index + 1].first);
  }
}

}  // namespace opossum
