#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "scheduler/task_queue.hpp"
#include "scheduler/worker.hpp"

namespace opossum {

class AbstractScheduler;

/**
 * Encapsulates the detection of the Machine Architecture, i.e. how many Nodes/Cores there are and how to distribute
 * Workers/Queues among them.
 */
class AbstractTopology {
 public:
  virtual ~AbstractTopology() = default;

  /**
   * Is expected to initialize all protected members
   */
  virtual void setup(AbstractScheduler &scheduler) = 0;

  const std::vector<std::shared_ptr<TaskQueue>> &queues() const;
  const std::vector<std::shared_ptr<Worker>> &workers() const;
  uint32_t num_nodes() const;
  uint32_t num_cores(uint32_t node_id) const;
  std::shared_ptr<TaskQueue> node_queue(uint32_t node_id);

 protected:
  std::vector<std::shared_ptr<TaskQueue>> _queues;
  std::vector<std::shared_ptr<Worker>> _workers;
  uint32_t _num_nodes;
  std::unordered_map<uint32_t, uint32_t> _num_cores_by_node_id;
};
}  // namespace opossum
