#include "fake_numa_topology.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <thread>

namespace opossum {

void FakeNumaTopology::setup(AbstractScheduler& scheduler) {
  auto max_num_threads = std::thread::hardware_concurrency();

  /**
   * Leave one thread free so hopefully the system won't overload and become unusable - but if we only have one thread,
   * use that one.
   */
  max_num_threads = std::max<uint32_t>(1, max_num_threads - 1);

  for (auto t = 0u; t < max_num_threads; t++) {
    auto queue = std::make_shared<TaskQueue>(t);
    auto worker = std::make_shared<Worker>(scheduler, queue, t, t);

    _queues.emplace_back(queue);
    _workers.emplace_back(worker);
  }
}
}  // namespace opossum
