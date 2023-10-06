#include "numa_utils.hpp"

#if HYRISE_NUMA_SUPPORT

#include <numa.h>

#endif

#include <numeric>
#include <string>

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace hyrise::numa_utils {

DistanceMatrix get_distance_matrix() {
  const auto num_nodes = Hyrise::get().topology.nodes().size();

  // 10 is the default distance to the same node.
  auto distance_matrix = DistanceMatrix(num_nodes, std::vector<int>(num_nodes, 10));

#if HYRISE_NUMA_SUPPORT

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

#endif

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

}  // namespace hyrise::numa_utils
