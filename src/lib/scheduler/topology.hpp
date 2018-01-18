#pragma once

#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "types.hpp"

namespace opossum {

struct TopologyCpu final {
  explicit TopologyCpu(CpuID cpu_id) : cpu_id(cpu_id) {}

  CpuID cpu_id = INVALID_CPU_ID;
};

struct TopologyNode final {
  explicit TopologyNode(std::vector<TopologyCpu>&& cpus) : cpus(std::move(cpus)) {}

  void print(std::ostream& stream = std::cout) const;

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
  static std::shared_ptr<Topology> create_numa_topology(uint32_t max_num_cores = 0);

  Topology(std::vector<TopologyNode>&& nodes, size_t num_cpus) : _nodes(std::move(nodes)), _num_cpus(num_cpus) {}

  const std::vector<TopologyNode>& nodes();

  size_t num_cpus() const;

  void print(std::ostream& stream = std::cout) const;

 private:
  std::vector<TopologyNode> _nodes;
  size_t _num_cpus;
};
}  // namespace opossum
