#include "tuning_choice.hpp"

namespace opossum {

std::shared_ptr<TuningOperation> TuningChoice::accept() const {
  if (is_currently_chosen()) {
    // No Operation
    return std::make_shared<TuningOperation>();
  } else {
    return _accept_operation();
  }
}

std::shared_ptr<TuningOperation> TuningChoice::reject() const {
  if (is_currently_chosen()) {
    return _reject_operation();
  } else {
    // No Operation
    return std::make_shared<TuningOperation>();
  }
}

void TuningChoice::print_on(std::ostream& output) const {
  output << "TuningChoice{desirability: " << desirability() << ", cost: " << cost()
         << ", chosen: " << is_currently_chosen() << "}";
}

}  // namespace opossum
