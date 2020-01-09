#pragma once

#include <vector>

#include "types.hpp"

class KnapsackSolver {
 public:
  std::vector<size_t> solve(const uint64_t memory_budget, const std::vector<uint64_t> values, const std::vector<uint64_t> costs);
};


#endif //HYRISE_KNAPSACK_SOLVER_HPP
