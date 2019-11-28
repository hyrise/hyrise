#pragma once

#include <memory>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/tracing/probes.hpp"
#include "worker.hpp"

namespace opossum {

class AbstractTask;
class TaskQueue;

class AbstractScheduler : public Noncopyable {
 public:
  virtual ~AbstractScheduler() = default;

  /**
   * Begin the schedulers lifecycle as the global Scheduler instance. In this method do work that can't be done before
   * the Scheduler isn't registered as the global instance
   */
  virtual void begin() = 0;

  virtual void wait_for_all_tasks() = 0;

  /**
   * Ends the schedulers lifecycle as the global Scheduler instance. This waits for all scheduled tasks to be finished,
   * and sets the scheduler to inactive.
   *
   * The caller of this method has to make sure that no other tasks can be scheduled from outside while this method
   * is being executed, otherwise those tasks might get lost.
   */
  virtual void finish() = 0;

  virtual bool active() const = 0;

  virtual const std::vector<std::shared_ptr<TaskQueue>>& queues() const = 0;

  virtual void schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id = CURRENT_NODE_ID,
                        SchedulePriority priority = SchedulePriority::Default) = 0;

  template <typename TaskType>
  void wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
    DebugAssert(([&]() {
                  for (auto& task : tasks) {
                    if (!task->is_scheduled()) {
                      return false;
                    }
                  }
                  return true;
                }()),
                "In order to wait for a task’s completion, it needs to have been scheduled first.");

    /**
   * In case wait_for_tasks() is called from a Task being executed in a Worker, let the Worker handle the join()-ing,
   * otherwise join right here
   */
    auto worker = Worker::get_this_thread_worker();
    if (worker) {
      worker->_wait_for_tasks(tasks);
    } else {
      for (auto& task : tasks) task->_join();
    }
  }

  template <typename TaskType>
  void schedule_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
    DTRACE_PROBE1(HYRISE, SCHEDULE_TASKS, tasks.size());
    for (auto& task : tasks) {
      DTRACE_PROBE2(HYRISE, TASKS, reinterpret_cast<uintptr_t>(&tasks), reinterpret_cast<uintptr_t>(task.get()));
      task->schedule();
    }
  }

  template <typename TaskType>
  void schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
    schedule_tasks(tasks);
    DTRACE_PROBE1(HYRISE, SCHEDULE_TASKS_AND_WAIT, tasks.size());
    wait_for_tasks(tasks);
  }
};

}  // namespace opossum
