#include "abstract_cost_estimator.hpp"

#include <queue>
#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

Cost AbstractCostEstimator::estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // Cost all operators in arbitrary order

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

    cost += _estimate_node_cost(current_node);
    bfs_queue.push(current_node->left_input());
    bfs_queue.push(current_node->right_input());
  }

  return cost;
}

}  // namespace opossum
