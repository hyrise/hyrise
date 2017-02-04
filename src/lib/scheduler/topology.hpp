#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "task_queue.hpp"
#include "types.hpp"

namespace opossum {

class AbstractScheduler;

struct TopologyCpu final {
  explicit TopologyCpu(CpuID cpuID) : cpuID(cpuID) {}

  CpuID cpuID = INVALID_CPU_ID;
};

struct TopologyNode final {
  explicit TopologyNode(std::vector<TopologyCpu>&& cpus) : cpus(std::move(cpus)) {}

  std::vector<TopologyCpu> cpus;
};

/**
 * Encapsulates the Machine Architecture, i.e. how many Nodes/Cores there are and how to distribute
 * Workers/Queues among them.
 */
class Topology final {
 public:
  /**
   * @param max_num_workers A values of zero indicates no limit
   * @param workers_per_node
   */
  static std::shared_ptr<Topology> create_fake_numa_topology(uint32_t max_num_workers = 0,
                                                             uint32_t workers_per_node = 1);
  static std::shared_ptr<Topology> create_numa_topology();

  Topology(std::vector<TopologyNode>&& nodes, size_t numCpus) : _nodes(std::move(nodes)), _numCpus(numCpus) {}

  const std::vector<TopologyNode>& nodes() { return _nodes; }

  size_t numCpus() const { return _numCpus; }

 private:
  std::vector<TopologyNode> _nodes;
  size_t _numCpus;
};
}  // namespace opossum
