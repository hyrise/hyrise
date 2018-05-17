#include "tuning_option.hpp"

#include "null_tuning_operation.hpp"

namespace opossum {

float TuningOption::accept_desirability() const {
  if (!is_currently_chosen()) {
    return desirability();
  }
  return 0.0f;
}

float TuningOption::reject_desirability() const {
  if (is_currently_chosen()) {
    return -desirability();
  }
  return 0.0f;
}

float TuningOption::current_cost() const {
  if (is_currently_chosen()) {
    return cost();
  }
  return 0.0f;
}

float TuningOption::accept_cost() const {
  if (!is_currently_chosen()) {
    return cost();
  }
  return 0.0f;
}

float TuningOption::reject_cost() const {
  if (is_currently_chosen()) {
    return -cost();
  }
  return 0.0f;
}

const std::vector<std::weak_ptr<TuningOption> >& TuningOption::invalidates() const { return _invalidates; }

void TuningOption::add_invalidate(std::shared_ptr<TuningOption> choice) { _invalidates.push_back(choice); }

std::shared_ptr<TuningOperation> TuningOption::accept() const {
  if (is_currently_chosen()) {
    // No Operation
    return std::make_shared<NullTuningOperation>();
  } else {
    return _accept_operation();
  }
}

std::shared_ptr<TuningOperation> TuningOption::reject() const {
  if (is_currently_chosen()) {
    return _reject_operation();
  } else {
    // No Operation
    return std::make_shared<NullTuningOperation>();
  }
}

void TuningOption::print_on(std::ostream& output) const {
  output << "TuningOption{desirability: " << desirability() << ", confidence: " << confidence() << ", cost: " << cost()
         << ", chosen: " << is_currently_chosen() << "}\n";
}

}  // namespace opossum
