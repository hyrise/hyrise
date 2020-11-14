#pragma once

#include <memory>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/tracing/probes.hpp"
#include "worker.hpp"

namespace opossum {

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

  // Schedules the given tasks for execution and returns immediately.
  // If no asynchronicity is needed, prefer schedule_and_wait_for_tasks.
  static void schedule_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  // Blocks until all specified tasks are completed.
  // If no asynchronicity is needed, prefer schedule_and_wait_for_tasks.
  static void wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  // Schedules the given tasks for execution and waits for them to complete before returning. Tasks may be reorganized
  // internally, e.g., to reduce the number of tasks being executed in parallel. See the implementation of
  // NodeQueueScheduler::_group_tasks for an example.
  void schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

 protected:
  // Internal helper method that adds predecessor/successor relationships between tasks to limit the degree of
  // parallelism and reduce scheduling overhead.
  virtual void _group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const;
};

}  // namespace opossum
