#include "index_tuning_option.hpp"

#include <memory>

#include "tuning/index/index_tuning_operation.hpp"

namespace opossum {

float IndexTuningOption::desirability() const { return saved_work; }

float IndexTuningOption::cost() const { return memory_cost; }

float IndexTuningOption::confidence() const { return 1.0f; }

bool IndexTuningOption::is_currently_chosen() const { return index_exists; }

void IndexTuningOption::print_on(std::ostream& output) const {
  output << "IndexTuningOption{on: " << column_ref << ", exists: " << index_exists << ", saved_work: " << saved_work
         << " RowScans, memory_cost: " << memory_cost << " MiB}";
}

std::shared_ptr<TuningOperation> IndexTuningOption::_accept_operation() const {
  return std::make_shared<IndexTuningOperation>(column_ref, type, true);
}

std::shared_ptr<TuningOperation> IndexTuningOption::_reject_operation() const {
  return std::make_shared<IndexTuningOperation>(column_ref, type, false);
}

}  // namespace opossum
