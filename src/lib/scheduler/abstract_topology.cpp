#include <memory>
#include <vector>

#include "abstract_topology.hpp"

namespace opossum {

const std::vector<std::shared_ptr<TaskQueue>> &AbstractTopology::queues() const { return _queues; }

const std::vector<std::shared_ptr<Worker>> &AbstractTopology::workers() const { return _workers; }

uint32_t AbstractTopology::num_nodes() const { return _num_nodes; }

uint32_t AbstractTopology::num_cores(uint32_t node_id) const {
  auto it = _num_cores_by_node_id.find(node_id);

#ifdef IS_DEBUG
  if (it == _num_cores_by_node_id.end()) {
    throw std::logic_error("No such node id");
  }
#endif

  return it->second;
}

std::shared_ptr<TaskQueue> AbstractTopology::node_queue(uint32_t node_id) {
  for (auto &queue : _queues) {
    if (queue->node_id() == node_id) {
      return queue;
    }
  }

#ifdef IS_DEBUG
  throw std::logic_error("Node id not found");
#endif

  return nullptr;
}
}  // namespace opossum
