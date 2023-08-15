#include "predicate_reordering_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace {
using namespace hyrise;  // NOLINT(build/namespaces)

// Returns whether a certain node is a "predicate-style" node, i.e., a node that can be moved freely within a predicate
// chain.
bool is_predicate_style_node(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type == LQPNodeType::Predicate) {
    return true;
  }

  // Validate can be seen as a Predicate on the MVCC column.
  if (node->type == LQPNodeType::Validate) {
    return true;
  }

  // Semi-/anti-joins also reduce the number of tuples and can be freely reordered within a chain of predicates. This
  // might place the join below a ValidateNode, but since it is not a "proper" join (i.e., one that returns columns
  // from multiple tables), the ValidateNode will still be able to operate on the semi join's output. However, we do not
  // reorder semi-/anti-joins with multiple join predicates. Though they also filter the relation, they cannot be
  // executed efficiently.
  if (node->type == LQPNodeType::Join) {
    const auto& join_node = static_cast<const JoinNode&>(*node);
    if (is_semi_or_anti_join(join_node.join_mode) && join_node.join_predicates().size() == 1) {
      return true;
    }
  }

  return false;
}

void reorder_predicates(const std::vector<std::shared_ptr<AbstractLQPNode>>& predicates,
                        const std::shared_ptr<const AbstractCostEstimator>& cost_estimator) {
  // Store original input and output.
  auto input = predicates.back()->left_input();
  const auto outputs = predicates.front()->outputs();
  const auto input_sides = predicates.front()->get_input_sides();

  // Estimate the cardinality of the input node once to cache its estimated statistics. Since we execute the reordering
  // recursively, we can be sure the plan below does not change anymore and input the estimations can be cached safely.
  const auto& cardinality_estimator = cost_estimator->cardinality_estimator;
  cardinality_estimator->estimate_cardinality(input);

  // To order the predicates, we want to favor the predicate with the lowest cost. We estimate the cost of each
  // individual predicate on top of the input LQP, i.e., predicates are estimated independently. In the past, we just
  // used the output cardinality. This turned out to be an oversimplification for finding a good order of scans and
  // joins with alike selectivity, where joins are more expensive in general. We experimented with the following
  // optimization goals:
  //
  //   0) min #out                         (Minimal output cardinality, baseline)
  //   1) max (#in - #out) / (cost - #out) ("Most filtered-out rows per cost")
  //   2) min cost                         (Minimal cost)
  //   3) min #out * cost                  (Minimal output cardinality with cost penalty, also tried +, log, sqrt, ...)
  //   4) min (cost - out) * p + #out      (Cost with a penalty for joins, chosen approach. p = 1.5 for joins, 1 else.)
  //
  // We ended up using the estimated cost to account for the fact that joins are more expensive than predicates. Also,
  // we add a penalty to the input cardinalities since joins always have more overhead than predicates. The factor was
  // derived experimentally, which is far from being a perfect solution, but still better than not incorporating join
  // overhead at all.
  auto nodes_and_costs = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, Cost>>{};
  nodes_and_costs.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    predicate->set_left_input(input);
    // Estimate the cardinality and cost of the predicate without caching. As the predicate order is not yet determined,
    // caching leads to wrong estimates in the cache.
    const auto output_cardinality = cardinality_estimator->estimate_cardinality(predicate, false);
    const auto estimated_cost = cost_estimator->estimate_node_cost(predicate, false) - output_cardinality;
    const auto penalty = predicate->type == LQPNodeType::Join ? PredicateReorderingRule::JOIN_PENALTY : 1;
    const auto weighted_cost = estimated_cost * penalty + output_cardinality;
    nodes_and_costs.emplace_back(predicate, weighted_cost);
  }

  // Untie predicates from LQP, so we can freely retie them.
  for (const auto& predicate : predicates) {
    lqp_remove_node(predicate, AllowRightInput::Yes);
  }

  // Sort in descending order. The "most beneficial" predicate (i.e., with the lowest cost) is a the end.
  std::sort(nodes_and_costs.begin(), nodes_and_costs.end(),
            [&](auto& left, auto& right) { return left.second > right.second; });

  // Ensure that nodes are chained correctly. The predicate at the vector end is placed after the input.
  nodes_and_costs.back().first->set_left_input(input);

  // The predicate at the vector begin is placed before the outputs.
  const auto output_count = outputs.size();
  for (auto output_idx = size_t{0}; output_idx < output_count; ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], nodes_and_costs.front().first);
  }

  // All predicates are placed as output of (i.e., "after") the next predicate in the vector.
  const auto predicate_count = nodes_and_costs.size() - 1;
  for (auto predicate_index = size_t{0}; predicate_index < predicate_count; ++predicate_index) {
    nodes_and_costs[predicate_index].first->set_left_input(nodes_and_costs[predicate_index + 1].first);
  }
}

void reorder_predicates_recursively(const std::shared_ptr<AbstractLQPNode>& node,
                                    const std::shared_ptr<AbstractCostEstimator>& cost_estimator,
                                    std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!node || visited_nodes.contains(node)) {
    return;
  }

  if (!is_predicate_style_node(node)) {
    reorder_predicates_recursively(node->left_input(), cost_estimator, visited_nodes);
    reorder_predicates_recursively(node->right_input(), cost_estimator, visited_nodes);
    visited_nodes.emplace(node);
    return;
  }

  // Gather adjacent PredicateNodes.
  auto predicate_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  auto current_node = node;

  while (is_predicate_style_node(current_node)) {
    // Once a node has multiple outputs, we are not talking about a predicate chain anymore. However, a new chain can
    // start here.
    if (current_node->outputs().size() > 1 && !predicate_nodes.empty()) {
      break;
    }

    predicate_nodes.emplace_back(current_node);
    current_node = current_node->left_input();
  }

  // Recursively reorder predicates below the current predicate chain. Doing so, subplans do not change anymore and the
  // CardinalityEstimator can cache their statistics. We used a top-down approach for predicate reordering in the past,
  // but repeated calls to the CardinalityEstimator without caching turned out to be a significant performance
  // bottleneck for short-running queries.
  const auto& deepest_predicate = predicate_nodes.back();
  reorder_predicates_recursively(deepest_predicate->left_input(), cost_estimator, visited_nodes);
  reorder_predicates_recursively(deepest_predicate->right_input(), cost_estimator, visited_nodes);
  visited_nodes.insert(predicate_nodes.cbegin(), predicate_nodes.cend());

  // A chain of predicates was found. Sort PredicateNodes in descending order with regards to the expected cost.
  if (predicate_nodes.size() > 1) {
    reorder_predicates(predicate_nodes, cost_estimator);
  }
}

}  // namespace

namespace hyrise {

std::string PredicateReorderingRule::name() const {
  static const auto name = std::string{"PredicateReorderingRule"};
  return name;
}

void PredicateReorderingRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // We keep track of reordered predicate nodes, so that this rule touches predicate nodes once only.
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  cost_estimator->cardinality_estimator->guarantee_bottom_up_construction();

  reorder_predicates_recursively(lqp_root, cost_estimator, visited_nodes);
}

}  // namespace hyrise
