#include "immediate_execution_scheduler.hpp"

namespace hyrise {

void ImmediateExecutionScheduler::begin() {}

void ImmediateExecutionScheduler::wait_for_all_tasks() {}

void ImmediateExecutionScheduler::finish() {}

bool ImmediateExecutionScheduler::active() const {
  return false;
}

const std::vector<std::shared_ptr<TaskQueue>>& ImmediateExecutionScheduler::queues() const {
  return _queues;
}

void ImmediateExecutionScheduler::schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id,
                                           SchedulePriority priority) {
  DebugAssert(task->is_scheduled(), "Don't call ImmediateExecutionScheduler::schedule(), call schedule() on the task");

  if (task->is_ready()) {
    task->execute();
  } else {
    // If a task is not yet ready, its predecessors must be executed first.
    for (const auto& predecessor_task : task->predecessors()) {
      predecessor_task.lock()->schedule();
    }
  }

  Assert(task->is_done(), "Task should have been executed by now.");
}

}  // namespace hyrise
