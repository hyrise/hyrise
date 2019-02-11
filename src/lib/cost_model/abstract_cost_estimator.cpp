#include "abstract_cost_estimator.hpp"

#include <queue>
#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

AbstractCostEstimator::AbstractCostEstimator(const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator):
  cardinality_estimator(cardinality_estimator) {

}

Cost AbstractCostEstimator::estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<CostEstimationCache>& cost_estimation_cache,
                                               const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache) const {
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

    cost += estimate_node_cost(current_node, cost_estimation_cache, cardinality_estimation_cache);
    bfs_queue.push(current_node->left_input());
    bfs_queue.push(current_node->right_input());
  }

  return cost;
}

}  // namespace opossum
