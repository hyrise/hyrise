#pragma once

#include <memory>
#include <vector>

#include "types.hpp"
#include "utils/create_ptr_aliases.hpp"

namespace opossum {

class AbstractTask;
class CurrentScheduler;
class TaskQueue;
class Topology;

class AbstractScheduler {
  friend class CurrentScheduler;

 public:
  explicit AbstractScheduler(TopologySPtr topology);
  virtual ~AbstractScheduler() = default;

  const TopologySPtr& topology() const;

  /**
   * Begin the schedulers lifecycle as the global Scheduler instance. In this method do work that can't be done before
   * the Scheduler isn't registered as the global instance
   */
  virtual void begin() = 0;

  virtual void finish() = 0;

  virtual const std::vector<TaskQueueSPtr>& queues() const = 0;

  virtual void schedule(AbstractTaskSPtr task, NodeID preferred_node_id = CURRENT_NODE_ID,
                        SchedulePriority priority = SchedulePriority::Normal) = 0;

 protected:
  TopologySPtr _topology;
};



}  // namespace opossum
