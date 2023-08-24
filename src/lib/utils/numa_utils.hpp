#pragma once

#include <vector>

#include "performance_warning.hpp"
#include "scheduler/abstract_task.hpp"
#include "types.hpp"

namespace hyrise::numa_utils {

using DistanceMatrix = std::vector<std::vector<int>>;
using NodePriorityMatrix = std::vector<std::vector<NodeID>>;

/*
    Returns a NxN matrix M where each element M[x,y] is the distance between
    Node x and Node y. Same Node distance should be equal to 10.
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
*/
DistanceMatrix get_distance_matrix();

/*
    Takes a distance matrix M[n,n] where element M[x,y] is the distance between
    Node x and Node y. Returns n vectors of size n. For each vector at position
    j, the first NodeID is the closest to j, ...,  and the last is the furthest
    based on the distance matrix.
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
NodePriorityMatrix make_node_priority_matrix(const DistanceMatrix& distance_matrix);

}  // namespace hyrise::numa_utils
