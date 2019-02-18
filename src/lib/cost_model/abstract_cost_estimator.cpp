#include "abstract_cost_estimator.hpp"

#include <queue>
#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

AbstractCostEstimator::AbstractCostEstimator(const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator):
  cardinality_estimator(cardinality_estimator) {

}

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
    if (cost_estimation_cache) {
      const auto cost_estimation_cache_iter = cost_estimation_cache->find(current_node);
      if (cost_estimation_cache_iter != cost_estimation_cache->end()) {
        auto subplan_already_visited = false;

        // Mark the whole subplan below current_node as visited, so Costs do not get added multiple times
        for (const auto& current_node_input : {current_node->left_input(), current_node->right_input()}) {
          if (!current_node_input) continue;

          visit_lqp(current_node_input, [&](const auto &node) {
            subplan_already_visited |= visited.find(node) != visited.end();
            return subplan_already_visited ? LQPVisitation::DoNotVisitInputs : LQPVisitation::VisitInputs;
          });
        }

        if (!subplan_already_visited) {
          for (const auto& current_node_input : {current_node->left_input(), current_node->right_input()}) {
            if (!current_node_input) continue;

            visit_lqp(current_node_input, [&](const auto &node) {
              visited.emplace(node);
              return LQPVisitation::VisitInputs;
            });
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
  if (cost_estimation_cache) {
    cost_estimation_cache->emplace(lqp, cost);
  }

  return cost;
}

}  // namespace opossum
