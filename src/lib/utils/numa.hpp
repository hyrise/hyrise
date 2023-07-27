#pragma once

#include <numa.h>
#include <numeric>
#include <vector>
#include "performance_warning.hpp"
#include "types.hpp"

namespace hyrise {

using DistanceMatrix = std::vector<std::vector<int>>;
using NodeMatrix = std::vector<std::vector<NodeID>>;

DistanceMatrix get_distance_matrix(int num_nodes);

NodeMatrix sort_relative_node_ids(DistanceMatrix distance_matrix);

}  // namespace hyrise