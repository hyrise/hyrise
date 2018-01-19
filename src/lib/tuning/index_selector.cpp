#include <algorithm>
#include <iostream>

#include "index_selector.hpp"

namespace opossum {

std::vector<IndexOperation> IndexSelector::select_indices(std::vector<IndexEvaluation> evaluations,
                                                          float memory_budget) {
  _operations.clear();
  _operations.reserve(evaluations.size());

  // sort evaluations by ascending desirability
  std::sort(evaluations.begin(), evaluations.end());

  float memory_consumption = 0.0f;
  for (const auto& index : evaluations) {
    if (index.exists) {
      memory_consumption += index.memory_cost;
    }
  }

  auto best_index = evaluations.end() - 1;
  auto worst_index = evaluations.begin();

  // ToDo(group01): research if this problem can actually be solved by a greedy algorithm

  while (best_index >= worst_index) {
    if (worst_index->desirablility < 0.0f && -worst_index->desirablility > best_index->desirablility) {
      // deleting worst index is more beneficial than creating best index
      if (worst_index->exists) {
        _operations.emplace_back(worst_index->table_name, worst_index->column_id, false);
        memory_consumption -= worst_index->memory_cost;
      }
      ++worst_index;
    } else {
      // best index is positive and more beneficial than removal of worst index
      if (best_index->exists) {
        --best_index;
      } else {
        // determine minimum desirability that must be sacrificed
        // to obtain enough memory for the new index
        float sacrificed_desirability = 0.0f;
        float obtained_memory = 0.0f;
        float required_memory = best_index->memory_cost + memory_consumption - memory_budget;
        auto sacrifice_index = worst_index;
        while (obtained_memory < required_memory && sacrifice_index != best_index) {
          if (sacrifice_index->exists) {
            sacrificed_desirability += sacrifice_index->desirablility;
            obtained_memory += sacrifice_index->memory_cost;
          }
          ++sacrifice_index;
        }
        if (obtained_memory >= required_memory && sacrificed_desirability <= best_index->desirablility) {
          // delete the previously selected indices, then create the better index
          for (auto delete_index = worst_index; delete_index != sacrifice_index; ++delete_index) {
            if (delete_index->exists) {
              _operations.emplace_back(delete_index->table_name, delete_index->column_id, false);
              memory_consumption -= delete_index->memory_cost;
            }
          }
          worst_index = sacrifice_index;
          _operations.emplace_back(best_index->table_name, best_index->column_id, true);
          memory_consumption += best_index->memory_cost;
        }
        --best_index;
      }
    }
  }

  return _operations;
}

}  // namespace opossum
