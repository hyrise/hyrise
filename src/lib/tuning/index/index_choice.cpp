#include "index_choice.hpp"

#include <memory>

#include "tuning/index/index_operation.hpp"

namespace opossum {

float IndexChoice::desirability() const { return saved_work; }

float IndexChoice::cost() const { return memory_cost; }

float IndexChoice::confidence() const { return 1.0f; }

bool IndexChoice::is_currently_chosen() const { return exists; }

void IndexChoice::print_on(std::ostream& output) const {
  output << "IndexChoice{on: " << column_ref << ", exists: " << exists << ", saved_work: " << saved_work
         << " RowScans, memory_cost: " << memory_cost << " MiB}";
}

std::shared_ptr<TuningOperation> IndexChoice::_accept_operation() const {
  return std::make_shared<IndexOperation>(column_ref, type, true);
}

std::shared_ptr<TuningOperation> IndexChoice::_reject_operation() const {
  return std::make_shared<IndexOperation>(column_ref, type, false);
}

}  // namespace opossum
