#pragma once

#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "types.hpp"

namespace opossum {

struct TopologyCpu final {
  explicit TopologyCpu(CpuID init_cpu_id) : cpu_id(init_cpu_id) {}

  CpuID cpu_id = INVALID_CPU_ID;
};

struct TopologyNode final {
  explicit TopologyNode(std::vector<TopologyCpu>&& init_cpus) : cpus(std::move(init_cpus)) {}

  std::vector<TopologyCpu> cpus;
};

std::ostream& operator<<(std::ostream& stream, const TopologyNode& topology_node);

/**
 * Topology is a singleton that encapsulates the Machine Architecture, i.e. how many Nodes/Cores there are.
 * It is initialized with the actual system topology by default, but can be newly initialized with a custom topology
 * if needed, e.g. for testing purposes.
 *
 * The static 'use_*_topology()' methods replace the current topology information by the new one, and should be used carefully.
 */
class Topology final : public Noncopyable {
 public:
  /**
   * Use the default system topology.
   *
   * Calls _init_default_topology() internally.
   * Calls _init_numa_topology() or _init_non_numa_topology() if on a NUMA or non-NUMA system respectively.
   */
  void use_default_topology(uint32_t max_num_cores = 0);

  /**
   * Use a NUMA topology.
   * The topology has a number of cores equal to either max_num_cores or the number of physically available cores,
   * whichever one is lower. If max_num_cores is lower than the number of available physical cores, all cores from node 0
   * are used before any cores from node 1 are used.
   *
   * Calls _init_numa_topology() internally.
   * Calls _init_fake_numa_topology() if on a non-NUMA system.
   */
  void use_numa_topology(uint32_t max_num_cores = 0);

  /**
   * Use a non-NUMA topology.
   * The topology has one node, and a number of cores equal to either max_num_cores or the number of physically
   * availyble cores, whichever one is lower.
   *
   * Calls _init_non_numa_topology() internally.
   */
  void use_non_numa_topology(uint32_t max_num_cores = 0);

  /**
   * Use a fake-NUMA topology.
   * The topology has a number of cores equal to either max_num_cores or the number of physically available cores,
   * whichever one is lower. Virtual NUMA nodes are created based on the workers_per_node parameter.
   *
   * Calls _init_fake_numa_topology() internally.
   */
  void use_fake_numa_topology(uint32_t max_num_workers = 0, uint32_t workers_per_node = 1);

  const std::vector<TopologyNode>& nodes() const;

  size_t num_cpus() const;

 private:
  Topology();

  friend std::ostream& operator<<(std::ostream& stream, const Topology& topology);
  friend class Hyrise;

  void _init_default_topology(uint32_t max_num_cores = 0);
  void _init_numa_topology(uint32_t max_num_cores = 0);
  void _init_non_numa_topology(uint32_t max_num_cores = 0);
  void _init_fake_numa_topology(uint32_t max_num_workers = 0, uint32_t workers_per_node = 1);

  void _clear();

  std::vector<TopologyNode> _nodes;
  uint32_t _num_cpus{0};
  bool _fake_numa_topology{false};
  bool _filtered_by_affinity{false};

  static const int _number_of_hardware_nodes;
};

std::ostream& operator<<(std::ostream& stream, const Topology& topology);

}  // namespace opossum
