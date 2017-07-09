#pragma once

#include <memory>
#include <vector>

#include "types.hpp"

namespace opossum {

class AbstractTask;
class CurrentScheduler;
class JobTask;
class TaskQueue;
class Topology;
class Worker;

class AbstractScheduler {
  friend class CurrentScheduler;

 public:
  explicit AbstractScheduler(std::shared_ptr<Topology> topology);
  virtual ~AbstractScheduler() = default;

  const std::shared_ptr<Topology>& topology() const;

  /**
   * Begin the schedulers lifecycle as the global Scheduler instance. In this method do work that can't be done before
   * the Scheduler isn't registered as the global instance
   */
  virtual void begin() = 0;

  virtual void finish() = 0;

  virtual const std::vector<std::shared_ptr<TaskQueue>>& queues() const = 0;

  virtual void schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id = CURRENT_NODE_ID,
                        SchedulePriority priority = SchedulePriority::Normal) = 0;

  // Helper functions - not globals for the sake of it
  template <typename T>
  static void schedule_tasks(const std::vector<std::shared_ptr<T>>& tasks);
  template <typename T>
  static void schedule_tasks_and_wait(const std::vector<std::shared_ptr<T>> tasks);

 protected:
  std::shared_ptr<Topology> _topology;
};

template <typename T>
void AbstractScheduler::schedule_tasks(const std::vector<std::shared_ptr<T>>& tasks) {
  for (auto& task : tasks) {
    task->schedule();
  }
}

template <typename T>
void AbstractScheduler::schedule_tasks_and_wait(const std::vector<std::shared_ptr<T>> tasks) {
  schedule_tasks(tasks);
  if (!tasks.empty()) {
    tasks.back()->join();
  }
}

}  // namespace opossum
