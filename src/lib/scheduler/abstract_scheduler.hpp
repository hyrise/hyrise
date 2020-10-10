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

  void wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  // TODO
  virtual void group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const;

  void schedule_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  void schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);
};

}  // namespace opossum
