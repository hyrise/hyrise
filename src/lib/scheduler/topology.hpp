#pragma once

#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/numa_memory_resource.hpp"
#include "utils/singleton.hpp"

namespace boost {
namespace container {
namespace pmr {
class memory_resource;
}
}  // namespace container
}  // namespace boost

namespace opossum {

struct TopologyCpu final {
  explicit TopologyCpu(CpuID cpu_id) : cpu_id(cpu_id) {}

  CpuID cpu_id = INVALID_CPU_ID;
};

struct TopologyNode final {
  explicit TopologyNode(std::vector<TopologyCpu>&& cpus) : cpus(std::move(cpus)) {}

  void print(std::ostream& stream = std::cout, size_t indent = 0) const;

  std::vector<TopologyCpu> cpus;
};

/**
 * Topology is a singleton that encapsulates the Machine Architecture, i.e. how many Nodes/Cores there are.
 * It is initialized with the actual system topology by default, but can be newly initialized with a custom topology
 * if needed, e.g. for testing purposes.
 *
 * The static 'use_*_topology()' methods replace the current topology information by the new one, and should be used carefully.
 */
class Topology final : public Singleton<Topology> {
 public:
  /**
   * Use the default system topology.
   *
   * Calls _init_default_topology() internally.
   * Calls _init_numa_topology() or _init_non_numa_topology() if on a NUMA or non-NUMA system respectively.
   */
  static void use_default_topology(uint32_t max_num_cores = 0);

  /**
   * Use a NUMA topology.
   * The topology has a number of cores equal to either max_num_cores or the number of physically availyble cores,
   * whichever one is lower. The cores are distributed among the available NUMA nodes in a way that a node is filled up
   * to it's maximum core count first, bevor a new node is added.
   *
   * Calls _init_numa_topology() internally.
   * Calls _init_fake_numa_topology() if on a non-NUMA system.
   */
  static void use_numa_topology(uint32_t max_num_cores = 0);

  /**
   * Use a non-NUMA topology.
   * The topology has one node, and a number of cores equal to either max_num_cores or the number of physically
   * availyble cores, whichever one is lower.
   *
   * Calls _init_non_numa_topology() internally.
   */
  static void use_non_numa_topology(uint32_t max_num_cores = 0);

  /**
   * Use a fake-NUMA topology.
   * The topology has a number of cores equal to either max_num_cores or the number of physically available cores,
   * whichever one is lower. Virtual NUMA nodes are created based on the workers_per_node parameter.
   *
   * Calls _init_fake_numa_topology() internally.
   */
  static void use_fake_numa_topology(uint32_t max_num_workers = 0, uint32_t workers_per_node = 1);

  const std::vector<TopologyNode>& nodes();

  size_t num_cpus() const;

  boost::container::pmr::memory_resource* get_memory_resource(int node_id);

  void print(std::ostream& stream = std::cout, size_t indent = 0) const;

 private:
  Topology();

  friend class Singleton;

  void _init_default_topology(uint32_t max_num_cores = 0);
  void _init_numa_topology(uint32_t max_num_cores = 0);
  void _init_non_numa_topology(uint32_t max_num_cores = 0);
  void _init_fake_numa_topology(uint32_t max_num_workers = 0, uint32_t workers_per_node = 1);

  void _clear();
  void _create_memory_resources();

  std::vector<TopologyNode> _nodes;
  size_t _num_cpus{0};
  bool _fake_numa_topology{false};

  static const int _number_of_hardware_nodes;

  std::vector<NUMAMemoryResource> _memory_resources;
};
}  // namespace opossum
