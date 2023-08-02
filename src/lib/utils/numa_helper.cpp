#include <string>

#include "numa_helper.hpp"
#include "utils/assert.hpp"

namespace hyrise {

DistanceMatrix get_distance_matrix(int num_nodes) {
  // 10 is the default distance to the same node.
  auto distance_matrix = DistanceMatrix(static_cast<size_t>(num_nodes), std::vector<int>(num_nodes, 10));

  // If numa_distance does not work (e.g. code is execute on Windows), 0 will be returned.
  // For non NUMA systems, we check the max possible numa node, as this will still work, when we have
  // a single node. If this call fails, return default matrix.
  if (numa_distance(num_nodes - 1, num_nodes - 1) == 0) {
    PerformanceWarning(
        "Distance between numa nodes could not be calculated. Falling back to default distance for every "
        "interconnect.");
    return distance_matrix;
  }

  for (auto node_x = int{0}; node_x < num_nodes; ++node_x) {
    for (auto node_y = int{0}; node_y < num_nodes; ++node_y) {
      // TODO(anyone): Assert that numa_distance is symmetric.
      // (e.g. we can also set dis_matrix[y][x] from the same call)
      distance_matrix[node_x][node_y] = numa_distance(node_x, node_y);
      DebugAssert(distance_matrix[node_x][node_y] != 0, "numa distance could not find distance between node " +
                                                            std::to_string(node_x) + " and node " +
                                                            std::to_string(node_y));
    }
  }

  return distance_matrix;
}

NodeMatrix sort_relative_node_ids(DistanceMatrix distance_matrix) {
  int matrix_size = distance_matrix.size();
  auto node_matrix = NodeMatrix(static_cast<size_t>(matrix_size), std::vector<NodeID>(matrix_size, NodeID{0}));

  for (auto node_id = int{0}; node_id < matrix_size; ++node_id) {
    std::iota(node_matrix[node_id].begin(), node_matrix[node_id].end(), 0);
    std::sort(node_matrix[node_id].begin(), node_matrix[node_id].end(),
              [&](auto l, auto r) -> bool { return distance_matrix[node_id][l] < distance_matrix[node_id][r]; });
  }
  return node_matrix;
}

void merge_node_placements(std::vector<NodeID>& node_placements, std::vector<std::shared_ptr<AbstractTask>>& jobs,
                           std::vector<std::optional<NodeID>>& non_scheduled_placements) {
  DebugAssert(node_placements.size() == 0, "node_placements should be empty.");
  auto total_size = non_scheduled_placements.size();
  node_placements.reserve(total_size);
  uint32_t task_index = 0;
  for (auto& job : jobs) {
    while (non_scheduled_placements[task_index].has_value()) {
      node_placements.emplace_back(non_scheduled_placements[task_index].value());
      task_index++;
    }
    node_placements.emplace_back(job->node_id());
    task_index++;
  }
  // Fill in the leftover local tasks.
  while (task_index < total_size) {
    if (non_scheduled_placements[task_index].has_value()) {
      node_placements.emplace_back(non_scheduled_placements[task_index].value());
    }
    task_index++;
  }
  DebugAssert(node_placements.size() == total_size,
              "node_placements contains " + std::to_string(node_placements.size()) +
                  " NodeIDs while it should contain " + std::to_string(total_size));
}

}  // namespace hyrise
