#pragma once

#include <numa.h>
#include <numeric>
#include <vector>
#include "performance_warning.hpp"
#include "types.hpp"

namespace hyrise {

using DistanceMatrix = std::vector<std::vector<int>>;
using NodeMatrix = std::vector<std::vector<NodeID>>;

/*
    Exemplary output for 8 Nodes:
    {
        {10, 16, 19, 16, 50, 50, 50, 50},
        {16, 10, 16, 19, 50, 50, 50, 50},
        {19, 16, 10, 16, 50, 50, 50, 50},
        {16, 19, 16, 10, 50, 50, 50, 50},
        {50, 50, 50, 50, 10, 16, 19, 16},
        {50, 50, 50, 50, 16, 10, 16, 19},
        {50, 50, 50, 50, 19, 16, 10, 16},
        {50, 50, 50, 50, 16, 19, 16, 10},
    }
    Same Node distance should be equal to 10.
*/
DistanceMatrix get_distance_matrix(int num_nodes);

/*
    Exemplary output for 8 Nodes:
    {
        {0, 1, 3, 2, 4, 5, 6, 7},
        {1, 0, 2, 3, 4, 5, 6, 7},
        {2, 1, 3, 0, 4, 5, 6, 7},
        {3, 0, 2, 1, 4, 5, 6, 7},
        {4, 5, 7, 6, 0, 1, 2, 3},
        {5, 4, 6, 7, 0, 1, 2, 3},
        {6, 5, 7, 4, 0, 1, 2, 3},
        {7, 4, 6, 5, 0, 1, 2, 3},
    }
*/
NodeMatrix sort_relative_node_ids(DistanceMatrix distance_matrix);

}  // namespace hyrise
