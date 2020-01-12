#include "knapsack_solver.hpp"

#include <limits>
#include <numeric>


/**
 * Simple implementation for approximating the knapsack problem. We assume, that the memory budget is so large, that in
 * practice it will not matter if an item more ore less is packed. That way the kanpsack can be filled in descending
 * order of value-cost-ratios.
 * @param memory_budget Memory budget available for selecting items (size of the knapsack)
 * @param values Values as in how valuable the item is (value of an item)
 * @param costs Memory required to store item (cost of an item)
 * @return Vector of indices of items having the highest value-cost ratio. The costs of the selected items are less than
 * or equal to memory_budget.
 */
std::vector<size_t> KnapsackSolver::solve(const uint64_t memory_budget, const std::vector<float> values, const std::vector<uint64_t> costs) {
  DebugAssert(values.size() == costs.size(), "Vectors 'values' and 'costs' must contain the same number of elements.");
  std::vector<size_t> indices(values.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::vector<float> values_cost_ratio(values.size());
  for (size_t index = 0, end = values.size(); index < end; ++ index) {
    uint64_t cost = costs[index];
    if (cost > 0) values_cost_ratio[index] = values[index] / costs[index];
    else values_cost_ratio[index] = std::numeric_limits<uint64_t>::max();
  }
  std::sort(indices.begin(), indices.end(), [&values_cost_ratio](const auto a, const auto b) {
    return values_cost_ratio[a] > values_cost_ratio[b];
  });

  std::vector<size_t> selected_indices;
  auto required_memory_of_selection = 0ul;
  for (const auto index : indices) {
    if (required_memory_of_selection + costs[index] < memory_budget) {
      selected_indices.emplace_back(index);
      required_memory_of_selection += costs[index];
    }
  }

  // returns indices of objects to pack
  return selected_indices;
}