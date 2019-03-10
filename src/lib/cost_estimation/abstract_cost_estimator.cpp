#include "abstract_cost_estimator.hpp"

#include <queue>
#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"

namespace opossum {

AbstractCostEstimator::AbstractCostEstimator(const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator)
    : cardinality_estimator(cardinality_estimator) {}

Cost AbstractCostEstimator::estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // Sum up the costs of all operators in the plan, while making sure to cost each operator exactly once, even in the
  // presence of diamond shapes.

  // Breadth-first iteration of plan
  auto bfs_queue = std::queue<std::shared_ptr<AbstractLQPNode>>{};
  auto visited = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};

  bfs_queue.emplace(lqp);

  auto cost = Cost{0};

  while (!bfs_queue.empty()) {
    const auto current_node = bfs_queue.front();
    bfs_queue.pop();
    if (!current_node || !visited.emplace(current_node).second) {
      continue;
    }

    const auto cached_cost = _get_subplan_cost_from_cache(current_node, visited);
    if (cached_cost) {
      // Cost for subplan successfully retrieved from cache, no need to look at the inputs of the current_node
      cost += *cached_cost;
      continue;
    } else {
      cost += estimate_node_cost(current_node);
      bfs_queue.push(current_node->left_input());
      bfs_queue.push(current_node->right_input());
    }
  }

  // Store cost in cache
  if (cost_estimation_cache.cost_by_lqp) {
    cost_estimation_cache.cost_by_lqp->emplace(lqp, cost);
  }

  return cost;
}

void AbstractCostEstimator::guarantee_bottom_up_construction() {
  cost_estimation_cache.cost_by_lqp.emplace();
  cardinality_estimator->guarantee_bottom_up_construction();
}

std::optional<Cost> AbstractCostEstimator::_get_subplan_cost_from_cache(
    const std::shared_ptr<AbstractLQPNode>& lqp, std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited) const {
  /**
   * Look up this subplan in cache, if a cache is present.
   * Needs to pay special attention to diamond shapes in the LQP to avoid costing nodes multiple times.
   */
  if (!cost_estimation_cache.cost_by_lqp) {
    return std::nullopt;
  }

  const auto cost_estimation_cache_iter = cost_estimation_cache.cost_by_lqp->find(lqp);
  if (cost_estimation_cache_iter == cost_estimation_cache.cost_by_lqp->end()) {
    return std::nullopt;
  }

  auto subplan_already_visited = false;

  auto subplan_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};

  // Check whether the cache entry can be used: This is only the case if the entire subplan has not yet been
  // visited. If any node in it has already has been visited, and we'd use the cache entry anyway, costs would get
  // counted twice
  for (const auto& current_node_input : {lqp->left_input(), lqp->right_input()}) {
    if (!current_node_input) continue;

    visit_lqp(current_node_input, [&](const auto& node) {
      subplan_already_visited |= visited.find(node) != visited.end();
      subplan_nodes.emplace_back(node);
      return subplan_already_visited ? LQPVisitation::DoNotVisitInputs : LQPVisitation::VisitInputs;
    });
  }

  if (subplan_already_visited) {
    return std::nullopt;
  }

  // The subplan is "clean"/unvisited, mark all nodes in it as "visited" and return the cost from the cache entry
  for (const auto& subplan_node : subplan_nodes) {
    visited.emplace(subplan_node);
  }

  return cost_estimation_cache_iter->second;
}

}  // namespace opossum
