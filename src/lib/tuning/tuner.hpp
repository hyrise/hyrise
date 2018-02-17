#pragma once

#include <chrono>
#include <limits>
#include <memory>
#include <string>

#include "scheduler/abstract_task.hpp"
#include "tuning/abstract_evaluator.hpp"
#include "tuning/abstract_selector.hpp"

namespace opossum {

/**
 * A Tuner encapsulates the process of analyzing the current system state and
 * performing specific modifications to optimize the systems performance.
 *
 * It uses AbstractEvaluators to generate TuningChoices which represent possible
 * modifications together with their expected performance impact and costs.
 * TuningChoices are transformed into a concrete operation sequence
 * by an AbstractSelector, that also considers a cost budget that may not be
 * exceeded at any point in the sequence.
 *
 * While executing the operation sequence, the Tuner considers a time budget
 * and stops the execution once it exceeds that budget.
 *
 * Both the time and cost budget (the latter being enforced by the AbstractSelector)
 * are initially infinite (i.e. disabled).
 */
class Tuner {
 public:
  using Runtime = std::chrono::duration<float, std::chrono::seconds::period>;
  using RuntimeClock = std::chrono::high_resolution_clock;

  enum class Status { Unknown, Running, Completed, EvaluationTimeout, SelectionTimeout, ExecutionTimeout, Timeout };

  static constexpr float NoBudget = std::numeric_limits<float>::infinity();

  Tuner();

  /**
   * Accessors for the AbstractEvaluators used for the tuning process.
   *
   * The Tuner takes ownership of the supplied evaluator. The same evaluator
   * instance may never be used in more than one Tuner.
   */
  void add_evaluator(std::unique_ptr<AbstractEvaluator>&& evaluator);
  void remove_evaluator(std::size_t index);
  const std::vector<std::unique_ptr<AbstractEvaluator>>& evaluators() const;

  /**
   * Accessors for the AbstractSelector used for the tuning process.
   *
   * The Tuner takes ownership of the supplied selector. The same selector
   * instance may never be used in more than one Tuner.
   */
  void set_selector(std::unique_ptr<AbstractSelector>&& selector);
  const std::unique_ptr<AbstractSelector>& selector() const;

  /**
   * Configures the time budgets for subsequent schedule() invocations.
   *
   * evaluate_budget: time budget for the evaluation phase
   * select_budget: time budget for the selection phase
   * execute_budget: time budget for the evaluation phase
   * budget: overall budget for all three phases
   *
   * A value of Tuner::NoBudget disables checking for a particular budget.
   *
   * Once an individual phase exceeds its local or the overall budget, it is stopped
   * at the earliest possible point and regardless of its completeness no subsequent
   * phase is started.
   *
   * Note that the evaluation and selection phases are likely to contain one or
   * few large and indivisible steps. Consequently the tuning process can run
   * significantly longer than the specified budget.
   */
  void set_time_budget(float budget, float execute_budget = NoBudget, float evaluate_budget = NoBudget,
                       float select_budget = NoBudget);
  float time_budget() const;
  float evaluate_time_budget() const;
  float select_time_budget() const;
  float execute_time_budget() const;

  /**
   * The cost budget is enforced by the AbstractSelector and has no
   * fixed interpretation.
   *
   * Its semantics depend on the AbstractEvaluators used which must
   * be compatible in this respect.
   *
   * A value of positive infinity disables cost budget checking.
   */
  void set_cost_budget(float budget);
  float cost_budget() const;

  /**
   * Schedules a new tuning process that consists of a set of three tasks
   * for each of the evaluate, select and execute phases.
   *
   * It is an error to call this method while a tuning process is
   * already scheduled, i.e. is_running() returns true.
   */
  void schedule_tuning_process();

  /**
   * Indicates whether there is a tuning process currently scheduled,
   * which is not yet complete.
   */
  bool is_running();

  /**
   * Indicates the status of the last scheduled tuning process
   */
  Status status();

  /**
   * Block the current thread until a running tuning process is finished.
   * Returns immediately if no tuning process is running.
   */
  void wait_for_completion();

 protected:
  void _evaluate();
  void _select();
  void _execute();

  void _log_choices();
  void _log_operations();

 protected:
  std::vector<std::unique_ptr<AbstractEvaluator>> _evaluators;
  std::unique_ptr<AbstractSelector> _selector;

  Runtime _time_budget;
  Runtime _evaluate_time_budget;
  Runtime _select_time_budget;
  Runtime _execute_time_budget;

  float _cost_budget;

  // members specific to one particular tuning process
  Status _status;
  std::shared_ptr<AbstractTask> _evaluate_task;
  std::shared_ptr<AbstractTask> _select_task;
  std::shared_ptr<AbstractTask> _execute_task;

  Runtime _remaining_time_budget;
  bool _time_budget_exceeded;

  std::vector<std::shared_ptr<TuningChoice>> _choices;
  std::vector<std::shared_ptr<TuningOperation>> _operations;
};

}  // namespace opossum
