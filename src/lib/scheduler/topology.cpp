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

Topology& Topology::current() {
  static Topology instance;
  return instance;
}

Topology::Topology() { init_default_topology(); }

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

void Topology::init_default_topology() {
#if !HYRISE_NUMA_SUPPORT
  init_non_numa_topology();
#else
  init_numa_topology();
#endif
}

void Topology::init_numa_topology(uint32_t max_num_cores) {
#if !HYRISE_NUMA_SUPPORT
  init_fake_numa_topology(max_num_cores);
#else

  if (numa_available() < 0) {
    return init_fake_numa_topology(max_num_cores);
  }

  _nodes.clear();
  _num_cpus = 0;

  auto max_node = numa_max_node();
  auto num_configured_cpus = static_cast<CpuID>(numa_num_configured_cpus());
  auto cpu_bitmask = numa_allocate_cpumask();
  auto core_count = uint32_t{0};

  for (auto node_id = 0; node_id <= max_node; node_id++) {
    if (max_num_cores == 0 || core_count < max_num_cores) {
      auto cpus = std::vector<TopologyCpu>();

      numa_node_to_cpus(node_id, cpu_bitmask);

      for (CpuID cpu_id{0}; cpu_id < num_configured_cpus; ++cpu_id) {
        if (numa_bitmask_isbitset(cpu_bitmask, cpu_id)) {
          if (max_num_cores == 0 || core_count < max_num_cores) {
            cpus.emplace_back(TopologyCpu(cpu_id));
            _num_cpus++;
          }
          core_count++;
        }
      }

      TopologyNode node(std::move(cpus));
      _nodes.emplace_back(std::move(node));
    }
  }

  numa_free_cpumask(cpu_bitmask);
#endif
}

void Topology::init_non_numa_topology(uint32_t max_num_cores) {
  _nodes.clear();
  _num_cpus = 0;

  auto max_num_threads = std::thread::hardware_concurrency();

  /**
   * Leave one thread free so hopefully the system won't freeze - but if we only have one thread, use that one.
   */
  _num_cpus = std::max<uint32_t>(1, max_num_threads - 1);
  if (max_num_cores != 0) {
    _num_cpus = std::min<uint32_t>(_num_cpus, max_num_cores);
  }

  auto cpus = std::vector<TopologyCpu>();

  for (auto cpu_id = CpuID{0}; cpu_id < _num_cpus; cpu_id++) {
    cpus.emplace_back(TopologyCpu(cpu_id));
  }

  auto node = TopologyNode(std::move(cpus));
  _nodes.emplace_back(std::move(node));
}

void Topology::init_fake_numa_topology(uint32_t max_num_workers, uint32_t workers_per_node) {
  _nodes.clear();
  _num_cpus = 0;

  auto max_num_threads = std::thread::hardware_concurrency();

  /**
   * Leave one thread free so hopefully the system won't freeze - but if we only have one thread, use that one.
   */
  auto num_workers = std::max<uint32_t>(1, max_num_threads - 1);
  if (max_num_workers != 0) {
    num_workers = std::min<uint32_t>(num_workers, max_num_workers);
  }

  auto num_nodes = num_workers / workers_per_node;
  if (num_workers % workers_per_node != 0) num_nodes++;

  _nodes.reserve(num_nodes);

  auto cpu_id = CpuID{0};

  for (auto node_id = uint32_t{0}; node_id < num_nodes; node_id++) {
    auto cpus = std::vector<TopologyCpu>();

    for (auto worker_id = uint32_t{0}; worker_id < workers_per_node && cpu_id < num_workers; worker_id++) {
      cpus.emplace_back(TopologyCpu(cpu_id));
      cpu_id++;
    }

    auto node = TopologyNode(std::move(cpus));

    _nodes.emplace_back(std::move(node));
  }

  _num_cpus = num_workers;
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
