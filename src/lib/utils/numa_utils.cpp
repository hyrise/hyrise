#include "numa_utils.hpp"

#include <string>

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace hyrise::numa_utils {

DistanceMatrix get_distance_matrix() {
  const auto num_nodes = Hyrise::get().topology.nodes().size();

  // 10 is the default distance to the same node.
  auto distance_matrix = DistanceMatrix(num_nodes, std::vector<int>(num_nodes, 10));

  // If numa_distance does not work (e.g. code is executed on Windows), 0 will be returned.
  // To determine, wether the numa distance can be correctly read on the system, we get the
  // distance between the two "max" nodes. If that distance is set, all other distances
  // should be set as well.
  if (numa_distance(num_nodes - 1, num_nodes - 1) == 0) {
    PerformanceWarning(
        "Distance between numa nodes could not be calculated. Falling back to default distance for every "
        "interconnect.");
    return distance_matrix;
  }

  for (auto node_x = size_t{0}; node_x < num_nodes; ++node_x) {
    for (auto node_y = size_t{0}; node_y < num_nodes; ++node_y) {
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

NodePriorityMatrix make_node_priority_matrix(const DistanceMatrix& distance_matrix) {
  const auto matrix_size = distance_matrix.size();
  auto node_matrix = NodePriorityMatrix(matrix_size, std::vector<NodeID>(matrix_size, NodeID{0}));

  for (auto node_id = size_t{0}; node_id < matrix_size; ++node_id) {
    std::iota(node_matrix[node_id].begin(), node_matrix[node_id].end(), 0);
    std::sort(node_matrix[node_id].begin(), node_matrix[node_id].end(),
              [&](auto l, auto r) -> bool { return distance_matrix[node_id][l] < distance_matrix[node_id][r]; });
  }
  return node_matrix;
}

void merge_node_placements(std::vector<NodeID>& node_placements, const std::vector<std::shared_ptr<AbstractTask>>& jobs,
                           const std::vector<std::optional<NodeID>>& non_scheduled_placements) {
  DebugAssert(node_placements.size() == 0, "node_placements should be empty.");

  const auto total_size = non_scheduled_placements.size();
  node_placements.reserve(total_size);

  auto jobs_iter = jobs.begin();
  const auto jobs_end = jobs.end();

  const auto place_job = [&]() {
    const auto& job = *jobs_iter;
    node_placements.emplace_back(job->node_id());
    ++jobs_iter;
  };

  for (const auto& placement : non_scheduled_placements) {
    if (placement)
      node_placements.emplace_back(*placement);
    else if (jobs_iter != jobs_end)
      place_job();
  }

  // Fill in the leftover local tasks.
  while (jobs_iter != jobs_end)
    place_job();

  DebugAssert(node_placements.size() == total_size,
              "node_placements contains " + std::to_string(node_placements.size()) +
                  " NodeIDs while it should contain " + std::to_string(total_size));
}

}  // namespace hyrise::numa_utils