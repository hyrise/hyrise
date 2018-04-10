#include "index_tuning_choice.hpp"

#include <memory>

#include "tuning/index/index_tuning_operation.hpp"

namespace opossum {

float IndexTuningChoice::desirability() const { return saved_work; }

float IndexTuningChoice::cost() const { return memory_cost; }

float IndexTuningChoice::confidence() const { return 1.0f; }

bool IndexTuningChoice::is_currently_chosen() const { return exists; }

void IndexTuningChoice::print_on(std::ostream& output) const {
  output << "IndexTuningChoice{on: " << column_ref << ", exists: " << exists << ", saved_work: " << saved_work
         << " RowScans, memory_cost: " << memory_cost << " MiB}";
}

std::shared_ptr<TuningOperation> IndexTuningChoice::_accept_operation() const {
  return std::make_shared<IndexTuningOperation>(column_ref, type, true);
}

std::shared_ptr<TuningOperation> IndexTuningChoice::_reject_operation() const {
  return std::make_shared<IndexTuningOperation>(column_ref, type, false);
}

}  // namespace opossum
