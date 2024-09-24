#include "abstract_scheduler.hpp"

#include <memory>
#include <unordered_set>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "utils/assert.hpp"

namespace hyrise {

void AbstractScheduler::wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  if constexpr (HYRISE_DEBUG) {
    for (const auto& task : tasks) {
      Assert(task->is_scheduled(), "In order to wait for a task’s completion, it must have been scheduled first.");
    }
  }

  // In case wait_for_tasks() is called from a task being executed in a worker, let the worker handle the join()-ing,
  // otherwise join right here.
  const auto worker = Worker::get_this_thread_worker();  // Only set for NodeQueueScheduler.
  if (worker) {
    worker->_wait_for_tasks(tasks);
  } else {
    for (const auto& task : tasks) {
      task->_join();
    }
  }
}

void AbstractScheduler::_group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  // Do nothing - grouping tasks is implementation-defined.
}

void AbstractScheduler::_schedule_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  if constexpr (HYRISE_DEBUG) {
    const auto task_set = std::unordered_set<std::shared_ptr<AbstractTask>>(tasks.begin(), tasks.end());

    for (const auto& task : tasks) {
      for (const auto& successor : task->successors()) {
        Assert(task_set.contains(successor.get().shared_from_this()),
               "Successors of scheduled tasks must also be part of the passed tasks.");
      }

      for (const auto& predecessor : task->predecessors()) {
        Assert(task_set.contains(predecessor.get().shared_from_this()),
               "Predecessors of scheduled tasks must also be part of the passed tasks.");
      }
    }
  }

  for (const auto& task : tasks) {
    task->schedule();
  }
}

void AbstractScheduler::schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  _group_tasks(tasks);
  _schedule_tasks(tasks);
  wait_for_tasks(tasks);
}

}  // namespace hyrise
