#if OPOSSUM_NUMA_SUPPORT

#include "numa_topology.hpp"

#include <numa.h>

#include <iostream>
#include <memory>

namespace opossum {

void NumaTopology::setup(AbstractScheduler &scheduler) {
  if (numa_available() < 0) {
    return;
  }

  // numa_max_node() e.g. returns 0 when there is 1 node, so we have to add 1 to obtain the number of nodes
  _num_nodes = numa_max_node() + 1;
  auto num_configured_cpus = numa_num_configured_cpus();
  auto cpu_bitmask = numa_bitmask_alloc(num_configured_cpus);

  for (uint32_t n = 0; n < _num_nodes; n++) {
    auto queue = std::make_shared<TaskQueue>(n);
    _queues.emplace_back(queue);
    _num_cores_by_node_id[n] = 0;

    numa_node_to_cpus(n, cpu_bitmask);

    for (int c = 0; c < num_configured_cpus; c++) {
      if (numa_bitmask_isbitset(cpu_bitmask, c)) {
        _workers.emplace_back(std::make_shared<Worker>(scheduler, queue, c, c));
        _num_cores_by_node_id[n]++;
      }
    }
  }
}
}  // namespace opossum

#endif
