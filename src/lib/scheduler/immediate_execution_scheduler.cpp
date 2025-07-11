#include "immediate_execution_scheduler.hpp"

#include <memory>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "scheduler/task_queue.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

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

void ImmediateExecutionScheduler::_schedule(std::shared_ptr<AbstractTask> task, NodeID /*preferred_node_id*/,
                                            SchedulePriority /*priority*/) {
  DebugAssert(task->is_scheduled(),
              "Do not call ImmediateExecutionScheduler::schedule(), call schedule() on the task.");

  if (task->is_ready()) {
    task->execute();
  } else {
    // If a task is not yet ready, its predecessors must be executed first.
    for (const auto& predecessor_task : task->predecessors()) {
      predecessor_task.get().schedule();
    }
  }

  Assert(task->is_done(), "Task should have been executed by now.");
}

void ImmediateExecutionScheduler::_group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  // Nothing to do in this scheduler.
}

}  // namespace hyrise
