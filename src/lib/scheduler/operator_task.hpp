#pragma once

#include <memory>

#include "operators/abstract_operator.hpp"

#include "scheduler/abstract_task.hpp"

namespace opossum {

/**
 * Makes an AbstractOperator scheduleable
 */
class OperatorTask : public AbstractTask {
 public:
  explicit OperatorTask(std::shared_ptr<AbstractOperator> op);

  const std::shared_ptr<AbstractOperator>& get_operator() const;

  /**
   * Takes arguments to create an AbstractOperator, and then creates a task for that operator.
   * It also sets the dependencies for the task based on the input operators, if any.
   */
  template <typename T, typename... ConstructorArgs>
  static std::shared_ptr<OperatorTask> make_from_operator_args(ConstructorArgs &&... args) {
    auto op = std::make_shared<T>(std::forward<ConstructorArgs>(args)...);
    return std::make_shared<OperatorTask>(op);
  }

  template <typename T, typename... ConstructorArgs>
  static std::shared_ptr<OperatorTask> make_from_operator_args(std::shared_ptr<OperatorTask> input_task,
                                                               ConstructorArgs &&... args) {
    auto op = std::make_shared<T>(input_task->get_operator(), std::forward<ConstructorArgs>(args)...);
    auto task = std::make_shared<OperatorTask>(op);
    input_task->set_as_predecessor_of(task);
    return task;
  }

  template <typename T, typename... ConstructorArgs>
  static std::shared_ptr<OperatorTask> make_from_operator_args(std::shared_ptr<OperatorTask> input_task_left,
                                                               std::shared_ptr<OperatorTask> input_task_right,
                                                               ConstructorArgs &&... args) {
    auto op = std::make_shared<T>(input_task_left->get_operator(), input_task_right->get_operator(),
                                  std::forward<ConstructorArgs>(args)...);
    auto task = std::make_shared<OperatorTask>(op);
    input_task_left->set_as_predecessor_of(task);
    input_task_right->set_as_predecessor_of(task);
    return task;
  }

 protected:
  void on_execute() override;

 private:
  std::shared_ptr<AbstractOperator> _op;
};
}  // namespace opossum
