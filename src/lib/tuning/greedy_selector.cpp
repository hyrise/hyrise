#include "greedy_selector.hpp"

#include <algorithm>

#include "utils/logging.hpp"

namespace opossum {

std::vector<std::shared_ptr<TuningOperation>> GreedySelector::select(std::vector<std::shared_ptr<TuningChoice>> choices,
                                                                     float memory_budget) {
  std::vector<std::shared_ptr<TuningOperation>> operations;
  operations.reserve(choices.size());

  if (choices.size() == 0) {
    return operations;
  }

  // sort evaluations by ascending desirability
  std::sort(choices.begin(), choices.end());

  float memory_consumption = 0.0f;
  for (const auto& index : choices) {
    if (index->is_currently_chosen()) {
      memory_consumption += index->cost();
    }
  }

  auto best_index = choices.end() - 1;
  auto worst_index = choices.begin();

  // ToDo(group01): research if this problem can actually be solved by a greedy algorithm

  while (best_index >= worst_index) {
    if ((*worst_index)->desirability() < 0.0f && -(*worst_index)->desirability() > (*best_index)->desirability()) {
      // deleting worst index is more beneficial than creating best index
      if ((*worst_index)->is_currently_chosen()) {
        LOG_DEBUG("Planned operation: delete worst existing index " << **worst_index);
        operations.push_back((*worst_index)->reject());
        memory_consumption -= (*worst_index)->cost();
      }
      ++worst_index;
    } else {
      // best index is positive and more beneficial than removal of worst index
      if ((*best_index)->is_currently_chosen()) {
        --best_index;
      } else {
        // determine minimum desirability that must be sacrificed
        // to obtain enough memory for the new index
        float sacrificed_desirability = 0.0f;
        float obtained_memory = 0.0f;
        float required_memory = (*best_index)->cost() + memory_consumption - memory_budget;
        auto sacrifice_index = worst_index;
        while (obtained_memory < required_memory && sacrifice_index != best_index) {
          if ((*sacrifice_index)->is_currently_chosen()) {
            sacrificed_desirability += (*sacrifice_index)->desirability();
            obtained_memory += (*sacrifice_index)->cost();
          }
          ++sacrifice_index;
        }
        if (obtained_memory >= required_memory && sacrificed_desirability <= (*best_index)->desirability()) {
          // delete the previously selected indices, then create the better index
          for (auto delete_index = worst_index; delete_index != sacrifice_index; ++delete_index) {
            if ((*delete_index)->is_currently_chosen()) {
              LOG_DEBUG("Planned operation: delete existing index " << *delete_index);
              operations.push_back((*delete_index)->reject());
              memory_consumption -= (*delete_index)->cost();
            }
          }
          worst_index = sacrifice_index;
          LOG_DEBUG("Planned operation: create new index " << *best_index);
          operations.push_back((*best_index)->accept());
          memory_consumption += (*best_index)->cost();
        }
        --best_index;
      }
    }
  }

  return operations;
}

}  // namespace opossum
