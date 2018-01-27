#include "topology.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

#include <algorithm>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace opossum {

void TopologyNode::print(std::ostream& stream) const {
  stream << "Number of Node CPUs: " << cpus.size() << ", CPUIDs: [";
  for (size_t cpu_idx = 0; cpu_idx < cpus.size(); ++cpu_idx) {
    stream << cpus[cpu_idx].cpu_id;
    if (cpu_idx + 1 < cpus.size()) {
      stream << ", ";
    }
  }
  stream << "]";
}

std::shared_ptr<Topology> Topology::create_fake_numa_topology(uint32_t max_num_workers, uint32_t workers_per_node) {
  auto max_num_threads = std::thread::hardware_concurrency();

  /**
   * Leave one thread free so hopefully the system won't freeze - but if we only have one thread, use that one.
   */
  auto num_workers = std::max<int32_t>(1, max_num_threads - 1);
  if (max_num_workers != 0) {
    num_workers = std::min<int32_t>(num_workers, max_num_workers);
  }

  auto num_nodes = num_workers / workers_per_node;
  if (num_workers % workers_per_node != 0) num_nodes++;

  std::vector<TopologyNode> nodes;
  nodes.reserve(num_nodes);

  CpuID cpu_id{0};

  for (auto n = 0u; n < num_nodes; n++) {
    std::vector<TopologyCpu> cpus;

    for (auto w = 0u; w < workers_per_node && cpu_id < num_workers; w++) {
      cpus.emplace_back(TopologyCpu(cpu_id));
      cpu_id++;
    }

    TopologyNode node(std::move(cpus));

    nodes.emplace_back(std::move(node));
  }

  return std::make_shared<Topology>(std::move(nodes), num_workers);
}

std::shared_ptr<Topology> Topology::create_numa_topology(uint32_t max_num_cores) {
#if !HYRISE_NUMA_SUPPORT
  return create_fake_numa_topology(max_num_cores);
#else

  if (numa_available() < 0) {
    return create_fake_numa_topology(max_num_cores);
  }

  auto max_node = numa_max_node();
  CpuID num_configured_cpus{numa_num_configured_cpus()};
  auto cpu_bitmask = numa_allocate_cpumask();
  uint32_t core_count = 0;

  std::vector<TopologyNode> nodes;

  for (int n = 0; n <= max_node; n++) {
    if (max_num_cores == 0 || core_count < max_num_cores) {
      std::vector<TopologyCpu> cpus;

      numa_node_to_cpus(n, cpu_bitmask);

      for (CpuID cpu_id{0}; cpu_id < num_configured_cpus; ++cpu_id) {
        if (numa_bitmask_isbitset(cpu_bitmask, cpu_id)) {
          if (max_num_cores == 0 || core_count < max_num_cores) {
            cpus.emplace_back(TopologyCpu(cpu_id));
          }
          core_count++;
        }
      }

      TopologyNode node(std::move(cpus));
      nodes.emplace_back(std::move(node));
    }
  }

  numa_free_cpumask(cpu_bitmask);
  return std::make_shared<Topology>(std::move(nodes), num_configured_cpus);
#endif
}

const std::vector<TopologyNode>& Topology::nodes() { return _nodes; }

size_t Topology::num_cpus() const { return _num_cpus; }

void Topology::print(std::ostream& stream) const {
  stream << "Number of CPUs: " << _num_cpus << std::endl;
  for (size_t node_idx = 0; node_idx < _nodes.size(); ++node_idx) {
    stream << "Node #" << node_idx << " - ";
    _nodes[node_idx].print(stream);
    stream << std::endl;
  }
}

}  // namespace opossum
