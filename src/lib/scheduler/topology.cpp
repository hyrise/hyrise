#include "topology.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

#include <algorithm>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include "utils/numa_memory_resource.hpp"

namespace opossum {

#if HYRISE_NUMA_SUPPORT
const int Topology::_number_of_hardware_nodes = numa_num_configured_nodes();  // NOLINT
#else
const int Topology::_number_of_hardware_nodes = 1;  // NOLINT
#endif

Topology::Topology() { _init_default_topology(); }

void TopologyNode::print(std::ostream& stream, size_t indent) const {
  for (size_t i = 0; i < indent; ++i) stream << " ";
  stream << "Number of Node CPUs: " << cpus.size() << ", CPUIDs: [";
  for (size_t cpu_idx = 0; cpu_idx < cpus.size(); ++cpu_idx) {
    for (size_t i = 0; i < indent; ++i) stream << " ";
    stream << cpus[cpu_idx].cpu_id;
    if (cpu_idx + 1 < cpus.size()) {
      stream << ", ";
    }
  }
  stream << "]";
}

void Topology::use_default_topology(uint32_t max_num_cores) { Topology::get()._init_default_topology(max_num_cores); }

void Topology::use_numa_topology(uint32_t max_num_cores) { Topology::get()._init_numa_topology(max_num_cores); }

void Topology::use_non_numa_topology(uint32_t max_num_cores) { Topology::get()._init_non_numa_topology(max_num_cores); }

void Topology::use_fake_numa_topology(uint32_t max_num_workers, uint32_t workers_per_node) {
  Topology::get()._init_fake_numa_topology(max_num_workers, workers_per_node);
}

void Topology::_init_default_topology(uint32_t max_num_cores) {
#if !HYRISE_NUMA_SUPPORT
  _init_non_numa_topology(max_num_cores);
#else
  _init_numa_topology(max_num_cores);
#endif
}

void Topology::_init_numa_topology(uint32_t max_num_cores) {
#if !HYRISE_NUMA_SUPPORT
  _init_fake_numa_topology(max_num_cores);
#else

  if (numa_available() < 0) {
    return _init_fake_numa_topology(max_num_cores);
  }

  _clear();
  _fake_numa_topology = false;

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

  _create_memory_resources();

  numa_free_cpumask(cpu_bitmask);
#endif
}

void Topology::_init_non_numa_topology(uint32_t max_num_cores) {
  _clear();
  _fake_numa_topology = false;

  _num_cpus = std::thread::hardware_concurrency();
  if (max_num_cores != 0) {
    _num_cpus = std::min<uint32_t>(_num_cpus, max_num_cores);
  }

  auto cpus = std::vector<TopologyCpu>();

  for (auto cpu_id = CpuID{0}; cpu_id < _num_cpus; cpu_id++) {
    cpus.emplace_back(TopologyCpu(cpu_id));
  }

  auto node = TopologyNode(std::move(cpus));
  _nodes.emplace_back(std::move(node));

  _create_memory_resources();
}

void Topology::_init_fake_numa_topology(uint32_t max_num_workers, uint32_t workers_per_node) {
  _clear();
  _fake_numa_topology = true;

  auto num_workers = std::thread::hardware_concurrency();
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
  _create_memory_resources();
}

const std::vector<TopologyNode>& Topology::nodes() { return _nodes; }

size_t Topology::num_cpus() const { return _num_cpus; }

boost::container::pmr::memory_resource* Topology::get_memory_resource(int node_id) {
  DebugAssert(node_id >= 0 && node_id < static_cast<int>(_nodes.size()), "node_id is out of bounds");
  return &_memory_resources[static_cast<size_t>(node_id)];
}

void Topology::print(std::ostream& stream, size_t indent) const {
  for (size_t i = 0; i < indent; ++i) stream << " ";
  stream << "Number of CPUs: " << _num_cpus << std::endl;
  for (size_t node_idx = 0; node_idx < _nodes.size(); ++node_idx) {
    for (size_t i = 0; i < indent; ++i) stream << " ";
    stream << "Node #" << node_idx << " - ";
    _nodes[node_idx].print(stream);
    stream << std::endl;
  }
}

void Topology::_clear() {
  _nodes.clear();
  _memory_resources.clear();
  _num_cpus = 0;
}

void Topology::_create_memory_resources() {
  for (auto node_id = size_t{0}; node_id < _nodes.size(); node_id++) {
    auto memsource_name = std::stringstream();
    memsource_name << "numa_" << std::setw(3) << std::setfill('0') << node_id;

    // If we have a fake NUMA topology that has more nodes than our system has available,
    // distribute the fake nodes among the physically available ones.
    auto system_node_id = _fake_numa_topology ? node_id % _number_of_hardware_nodes : node_id;
    _memory_resources.emplace_back(NUMAMemoryResource(system_node_id, memsource_name.str()));
  }
}

}  // namespace opossum
