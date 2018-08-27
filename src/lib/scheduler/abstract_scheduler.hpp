#pragma once

#include <memory>
#include <vector>

#include "types.hpp"

namespace opossum {

class AbstractTask;
class CurrentScheduler;
class TaskQueue;

class AbstractScheduler {
  friend class CurrentScheduler;

 public:
  virtual ~AbstractScheduler() = default;

  /**
   * Begin the schedulers lifecycle as the global Scheduler instance. In this method do work that can't be done before
   * the Scheduler isn't registered as the global instance
   */
  virtual void begin() = 0;

  virtual void finish() = 0;

  virtual const std::vector<std::shared_ptr<TaskQueue>>& queues() const = 0;

  virtual void schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id = CURRENT_NODE_ID,
                        SchedulePriority priority = SchedulePriority::Default) = 0;
};

}  // namespace opossum
