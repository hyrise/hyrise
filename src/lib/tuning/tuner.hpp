#pragma once

#include <memory>
#include <string>

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
  Tuner();

  /**
   * Accessors for the AbstractEvaluators used for the tuning process.
   */
  void add_evaluator(std::unique_ptr<AbstractEvaluator>&& evaluator);
  void remove_evaluator(std::size_t index);
  const std::vector<std::unique_ptr<AbstractEvaluator>>& evaluators() const;

  /**
   * Accessors for the AbstractSelector used for the tuning process.
   */
  void set_selector(std::unique_ptr<AbstractSelector>&& selector);
  const std::unique_ptr<AbstractSelector>& selector() const;

  /**
   * The time budget (in seconds) limits the time available for
   * system modification operations.
   *
   * The time consumed by the AbstractEvaluator and -Selector is
   * not counted against this budget.
   *
   * A value of positive infinity disables time budget checking.
   */
  void set_time_budget(float budget);
  void disable_time_budget();
  float time_budget() const;

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
  void disable_cost_budget();
  float cost_budget() const;

  /**
   * Initiates the tuning process
   */
  void execute();

 protected:
  void _execute_operations(std::vector<std::shared_ptr<TuningOperation>>& operations);

 protected:
  std::vector<std::unique_ptr<AbstractEvaluator>> _evaluators;
  std::unique_ptr<AbstractSelector> _selector;

  float _time_budget;
  float _cost_budget;
};

}  // namespace opossum
