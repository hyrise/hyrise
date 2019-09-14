#include "immediate_execution_scheduler.hpp"

namespace opossum {

void ImmediateExecutionScheduler::begin() {}

void ImmediateExecutionScheduler::wait_for_all_tasks() {}

void ImmediateExecutionScheduler::finish() {}

bool ImmediateExecutionScheduler::active() const { return false; }

const std::vector<std::shared_ptr<TaskQueue>>& ImmediateExecutionScheduler::queues() const { return _queues; }

void ImmediateExecutionScheduler::schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id,
                                           SchedulePriority priority) {
  // If the task is not ready yet, it will be executed once its predecessors are done.
  if (task->is_ready()) task->execute();
}

}  // namespace opossum
