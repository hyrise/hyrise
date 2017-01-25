#pragma once

#include <memory>

#include "abstract_task.hpp"

namespace opossum {

class AbstractTopology;

/**
 * Defines the interface for a Scheduler, allows for multiple (possibly experimental) Scheduler implementations
 */
class AbstractScheduler {
 public:
  explicit AbstractScheduler(std::shared_ptr<AbstractTopology> topology);
  virtual ~AbstractScheduler() = default;

  const std::shared_ptr<AbstractTopology>& topology() const;

  /**
   * Blocks until all queues are empty and all Tasks are finished
   */
  virtual void finish() = 0;

  virtual void schedule(std::shared_ptr<AbstractTask> task, uint32_t preferred_node_id) = 0;

 protected:
  std::shared_ptr<AbstractTopology> _topology;
};

}  // namespace opossum
