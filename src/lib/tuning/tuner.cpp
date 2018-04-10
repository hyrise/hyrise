#include "tuner.hpp"

#include "scheduler/job_task.hpp"

#include "utils/assert.hpp"

namespace opossum {

Tuner::Tuner()
    : _time_budget{NoBudget},
      _evaluate_time_budget{NoBudget},
      _select_time_budget{NoBudget},
      _execute_time_budget{NoBudget},
      _cost_budget{NoBudget},
      _status{Status::Unknown} {}

void Tuner::add_evaluator(std::unique_ptr<AbstractEvaluator>&& evaluator) {
  Assert(evaluator, "Tried to add an invalid evaluator.");
  _evaluators.push_back(std::move(evaluator));
}

void Tuner::remove_evaluator(std::size_t index) {
  if (index < _evaluators.size()) {
    _evaluators.erase(_evaluators.begin() + index);
    return;
  }
  Fail("index out of bounds");
}

const std::vector<std::unique_ptr<AbstractEvaluator>>& Tuner::evaluators() const { return _evaluators; }

void Tuner::set_selector(std::unique_ptr<AbstractSelector>&& selector) {
  Assert(selector, "Tried to add an invalid selector.");
  _selector = std::move(selector);
}

const std::unique_ptr<AbstractSelector>& Tuner::selector() const { return _selector; }

void Tuner::set_time_budget(float budget, float execute_budget, float evaluate_budget, float select_budget) {
  _time_budget = Runtime{budget};
  _evaluate_time_budget = Runtime{evaluate_budget};
  _select_time_budget = Runtime{select_budget};
  _execute_time_budget = Runtime{execute_budget};
}

float Tuner::time_budget() const { return _time_budget.count(); }

float Tuner::evaluate_time_budget() const { return _evaluate_time_budget.count(); }

float Tuner::select_time_budget() const { return _select_time_budget.count(); }

float Tuner::execute_time_budget() const { return _execute_time_budget.count(); }

void Tuner::set_cost_budget(float budget) { _cost_budget = budget; }

float Tuner::cost_budget() const { return _cost_budget; }

void Tuner::schedule_tuning_process() {
  Assert(_evaluators.size() > 0, "Can not run Tuner without at least one AbstractEvaluator");
  Assert(_selector, "Can not run Tuner without an AbstractSelector");
  Assert(!is_running(), "Can not schedule another tuning process while the previous process is still running");

  _evaluate_task = std::make_shared<JobTask>([this]() { _evaluate(); });
  _select_task = std::make_shared<JobTask>([this]() { _select(); });
  _execute_task = std::make_shared<JobTask>([this]() { _execute(); });

  _evaluate_task->set_as_predecessor_of(_select_task);
  _select_task->set_as_predecessor_of(_execute_task);

  _status = Status::Running;
  _remaining_time_budget = _time_budget;
  _time_budget_exceeded = false;

  _evaluate_task->schedule();
  _select_task->schedule();
  _execute_task->schedule();
}

bool Tuner::is_running() const {
  return (_evaluate_task && !_evaluate_task->is_done()) || (_select_task && !_select_task->is_done()) ||
         (_execute_task && !_execute_task->is_done());
}

Tuner::Status Tuner::status() const { return _status; }

void Tuner::wait_for_completion() {
  if (_evaluate_task) {
    _evaluate_task->join();
  }
  if (_select_task) {
    _select_task->join();
  }
  if (_execute_task) {
    _execute_task->join();
  }
}

void Tuner::_evaluate() {
  const auto begin = RuntimeClock::now();

  _choices.clear();
  for (const auto& evaluator : _evaluators) {
    evaluator->evaluate(_choices);

    const auto runtime = RuntimeClock::now() - begin;
    if (runtime > _remaining_time_budget) {
      _status = Status::Timeout;
      return;
    }
    if (runtime > _evaluate_time_budget) {
      _status = Status::EvaluationTimeout;
      return;
    }
  }
  _remaining_time_budget -= RuntimeClock::now() - begin;
}

void Tuner::_select() {
  if (_status != Status::Running) {
    return;
  }

  const auto begin = RuntimeClock::now();

  _operations = _selector->select(_choices, _cost_budget);

  const auto runtime = RuntimeClock::now() - begin;
  if (runtime > _remaining_time_budget) {
    _status = Status::Timeout;
    return;
  }
  if (runtime > _select_time_budget) {
    _status = Status::SelectionTimeout;
    return;
  }
  _remaining_time_budget -= runtime;
}

void Tuner::_execute() {
  if (_status != Status::Running) {
    return;
  }

  const auto begin = RuntimeClock::now();

  for (auto& operation : _operations) {
    operation->execute();

    const auto runtime = RuntimeClock::now() - begin;
    if (runtime > _remaining_time_budget) {
      _status = Status::Timeout;
      return;
    }
    if (runtime > _execute_time_budget) {
      _status = Status::ExecutionTimeout;
      return;
    }
  }

  _status = Status::Completed;
}

}  // namespace opossum
