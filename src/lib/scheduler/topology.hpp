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
 * Topology is a singleton that encapsulates the Machine Architecture, i.e. how many Nodes/Cores there are.
 * It is initialized with the actual system topology by default, but can be newly initialized with a custom topology
 * if needed, e.g. for testing purposes.
 *
 * The 'init_*_topology()' methods replace the current topology information by the new one, and should be used carefully.
 */
class Topology final {
 public:
  static Topology& current();

  /**
   * Initialize with the default system topology.
   *
   * Calls init_numa_topology() or init_non_numa_topology() if on a NUMA or non-NUMA system respectively.
   */
  void init_default_topology();

  /**
   * Initialize with a NUMA topology.
   * The topology has a number of cores equal to either max_num_cores or the number of physically availyble cores,
   * whichever one is lower. The cores are distributed among the available NUMA nodes in a way that a node is filled up
   * to it's maximum core count first, bevor a new node is added.
   *
   * Calls init_fake_numa_topology() if on a non-NUMA system.
   */
  void init_numa_topology(uint32_t max_num_cores = 0);

  /**
   * Initialize with a non-NUMA topology.
   * The topology has one node, and a number of cores equal to either max_num_cores or the number of physically
   * availyble cores, whichever one is lower.
   */
  void init_non_numa_topology(uint32_t max_num_cores = 0);

  /**
   * Initialize with a fake-NUMA topology.
   * The topology has a number of cores equal to either max_num_cores or the number of physically availyble cores,
   * whichever one is lower. Virtual NUMA nodes are created based on the workers_per_node parameter.
   */
  void init_fake_numa_topology(uint32_t max_num_workers = 0, uint32_t workers_per_node = 1);

  const std::vector<TopologyNode>& nodes();

  size_t num_cpus() const;

  void print(std::ostream& stream = std::cout) const;

 private:
  Topology();

  std::vector<TopologyNode> _nodes;
  size_t _num_cpus;
};
}  // namespace opossum
