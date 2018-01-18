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

  // ToDo(group01): This is still not optimal as several less desirable indices
  // might be sacrificed for a more desirable but larger index even though their
  // cumulative desirability might be larger than that of the created index

  while (best_index >= worst_index) {
    if (worst_index->desirablility < 0.0f && -worst_index->desirablility > best_index->desirablility) {
      // deleting worst index is more beneficial than creating best index
      if (worst_index->exists) {
        _operations.emplace_back(worst_index->table_name, worst_index->column_id, false);
        memory_consumption -= worst_index->memory_cost;
      }
      ++worst_index;
    } else {
      // best index must be positive and more beneficial than removal of worst index
      if (best_index->exists) {
        --best_index;
      } else {
        if (memory_consumption + best_index->memory_cost < memory_budget) {
          // create best index as it fits into memory budget
          _operations.emplace_back(best_index->table_name, best_index->column_id, true);
          memory_consumption += best_index->memory_cost;
          --best_index;
        } else {
          // worst index is deleted to make space for better (=best) index
          if (worst_index->exists) {
            _operations.emplace_back(worst_index->table_name, worst_index->column_id, false);
            memory_consumption -= worst_index->memory_cost;
          }
          ++worst_index;
        }
      }
    }
  }

  return _operations;
}

}  // namespace opossum
