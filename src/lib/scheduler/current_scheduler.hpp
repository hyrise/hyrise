#pragma once

#include <memory>
#include <vector>

#include "utils/assert.hpp"
#include "utils/tracing/probes.hpp"
#include "worker.hpp"

namespace opossum {

class AbstractScheduler;

/**
 * Holds the singleton instance (or the lack of one) of the currently active Scheduler
 */
class CurrentScheduler {
 public:
  inline static const std::shared_ptr<AbstractScheduler>& get() { return _instance; }
  static void set(const std::shared_ptr<AbstractScheduler>& instance);

  /**
   * The System runs without a Scheduler in most Tests and with one almost everywhere else. Tasks need to work
   * regardless of a Scheduler existing or not, use this method to query its existence.
   */
  static bool is_set();

  /**
   * If there is an active Scheduler, block execution until all @tasks have finished
   * If there is no active Scheduler, returns immediately since all @tasks have executed when they were scheduled
   */
  template <typename TaskType>
  static void wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks);

  template <typename TaskType>
  static void schedule_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks);

  template <typename TaskType>
  static void schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks);

 private:
  static std::shared_ptr<AbstractScheduler> _instance;
};

template <typename TaskType>
void CurrentScheduler::wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
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
void CurrentScheduler::schedule_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
  DTRACE_PROBE1(HYRISE, SCHEDULE_TASKS, tasks.size());
  for (auto& task : tasks) {
    DTRACE_PROBE2(HYRISE, TASKS, reinterpret_cast<uintptr_t>(&tasks), reinterpret_cast<uintptr_t>(task.get()));
    task->schedule();
  }
}

template <typename TaskType>
void CurrentScheduler::schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
  schedule_tasks(tasks);
  DTRACE_PROBE1(HYRISE, SCHEDULE_TASKS_AND_WAIT, tasks.size());
  wait_for_tasks(tasks);
}

}  // namespace opossum
