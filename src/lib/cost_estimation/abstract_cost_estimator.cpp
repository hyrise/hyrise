#include "abstract_cost_estimator.hpp"

#include <queue>
#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

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

    // Look up this subplan in cache, if a cache is present
    if (cost_estimation_cache.cost_by_lqp) {
      const auto cost_estimation_cache_iter = cost_estimation_cache.cost_by_lqp->find(current_node);
      if (cost_estimation_cache_iter != cost_estimation_cache.cost_by_lqp->end()) {
        auto subplan_already_visited = false;

        auto subplan_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};

        // Check whether the cache entry can be used: This is only the case if the entire subplan has not yet been
        // visited. If it already has been visited, and we'd use the cache entry anyway, costs would get counted twice
        for (const auto& current_node_input : {current_node->left_input(), current_node->right_input()}) {
          if (!current_node_input) continue;

          visit_lqp(current_node_input, [&](const auto& node) {
            subplan_already_visited |= visited.find(node) != visited.end();
            subplan_nodes.emplace_back(node);
            return subplan_already_visited ? LQPVisitation::DoNotVisitInputs : LQPVisitation::VisitInputs;
          });
        }

        // If the subplan is "clean", mark all nodes in it as "visited" and add the cost from the cache entry to the
        // counter
        if (!subplan_already_visited) {
          for (const auto& subplan_node : subplan_nodes) {
            visited.emplace(subplan_node);
          }
          cost += cost_estimation_cache_iter->second;
          continue;
        }
      }
    }

    cost += estimate_node_cost(current_node);
    bfs_queue.push(current_node->left_input());
    bfs_queue.push(current_node->right_input());
  }

  // Store cost in cache
  if (cost_estimation_cache.cost_by_lqp) {
    cost_estimation_cache.cost_by_lqp->emplace(lqp, cost);
  }

  return cost;
}

}  // namespace opossum
