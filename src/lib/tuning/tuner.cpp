#include "tuner.hpp"

#include <chrono>
#include <limits>

#include "utils/assert.hpp"
#include "utils/logging.hpp"

namespace opossum {

Tuner::Tuner()
    : _time_budget{std::numeric_limits<float>::infinity()}, _cost_budget{std::numeric_limits<float>::infinity()} {}

void Tuner::add_evaluator(std::unique_ptr<AbstractEvaluator>&& evaluator) {
  _evaluators.push_back(std::move(evaluator));
}

void Tuner::remove_evaluator(std::size_t index) {
  if (index < _evaluators.size()) {
    _evaluators.erase(_evaluators.begin() + index);
  }
}

const std::vector<std::unique_ptr<AbstractEvaluator>>& Tuner::evaluators() const { return _evaluators; }

void Tuner::set_selector(std::unique_ptr<AbstractSelector>&& selector) { _selector = std::move(selector); }

const std::unique_ptr<AbstractSelector>& Tuner::selector() const { return _selector; }

void Tuner::set_time_budget(float budget) { _time_budget = budget; }

void Tuner::disable_time_budget() { _time_budget = std::numeric_limits<float>::infinity(); }

float Tuner::time_budget() const { return _time_budget; }

void Tuner::set_cost_budget(float budget) { _cost_budget = budget; }

void Tuner::disable_cost_budget() { _cost_budget = std::numeric_limits<float>::infinity(); }

float Tuner::cost_budget() const { return _cost_budget; }

void Tuner::execute() {
  Assert(_evaluators.size() > 0, "Can not run Tuner without at least one AbstractEvaluator");
  Assert(_selector, "Can not run Tuner without an AbstractSelector");

  std::vector<std::shared_ptr<TuningChoice>> choices;
  LOG_INFO("Running evaluators...");
  for (const auto& evaluator : _evaluators) {
    if (evaluator) {
      evaluator->evaluate(choices);
    } else {
      LOG_WARN("Found null evaluator");
    }
  }

  for (const auto& choice : choices) {
    LOG_DEBUG("-> " << *choice);
    (void)choice; // Silence warning about unused variable in release builds
  }

  LOG_INFO("Running Selector...");
  std::vector<std::shared_ptr<TuningOperation>> operations = _selector->select(choices, _cost_budget);

  LOG_DEBUG("Operation sequence:");
  for (const auto& operation : operations) {
    LOG_DEBUG("-> " << *operation);
    (void)operation; // Silence warning about unused variable in release builds
  }

  LOG_INFO("Executing operations...");
  _execute_operations(operations);

  LOG_INFO("Tuning completed.");
}

void Tuner::_execute_operations(std::vector<std::shared_ptr<TuningOperation>>& operations) {
  std::chrono::duration<float, std::chrono::seconds::period> timeout{_time_budget};
  auto begin = std::chrono::high_resolution_clock::now();

  for (auto& operation : operations) {
    LOG_INFO("Executing " << *operation);
    operation->execute();
    auto end = std::chrono::high_resolution_clock::now();
    if (end - begin > timeout) {
      LOG_INFO("Execution incomplete: Time budget exceeded.");
      break;
    }
  }
}

}  // namespace opossum
