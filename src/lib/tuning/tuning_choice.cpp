#include "tuning_choice.hpp"

#include "null_operation.hpp"

namespace opossum {

float TuningChoice::accept_desirability() const {
  if (!is_currently_chosen()) {
    return desirability();
  }
  return 0.0f;
}

float TuningChoice::reject_desirability() const {
  if (is_currently_chosen()) {
    return -desirability();
  }
  return 0.0f;
}

float TuningChoice::current_cost() const {
  if (is_currently_chosen()) {
    return cost();
  }
  return 0.0f;
}

float TuningChoice::accept_cost() const {
  if (!is_currently_chosen()) {
    return cost();
  }
  return 0.0f;
}

float TuningChoice::reject_cost() const {
  if (is_currently_chosen()) {
    return -cost();
  }
  return 0.0f;
}

const std::set<std::shared_ptr<TuningChoice> >& TuningChoice::invalidates() const { return _invalidates; }

void TuningChoice::add_invalidate(std::shared_ptr<TuningChoice> choice) { _invalidates.insert(choice); }

std::shared_ptr<TuningOperation> TuningChoice::accept() const {
  if (is_currently_chosen()) {
    // No Operation
    return std::make_shared<NullOperation>();
  } else {
    return _accept_operation();
  }
}

std::shared_ptr<TuningOperation> TuningChoice::reject() const {
  if (is_currently_chosen()) {
    return _reject_operation();
  } else {
    // No Operation
    return std::make_shared<NullOperation>();
  }
}

void TuningChoice::print_on(std::ostream& output) const {
  output << "TuningChoice{desirability: " << desirability() << ", confidence: " << confidence() << ", cost: " << cost()
         << ", chosen: " << is_currently_chosen() << "}\n";
}

}  // namespace opossum
