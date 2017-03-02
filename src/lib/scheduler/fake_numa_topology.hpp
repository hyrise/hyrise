#pragma once

#include "abstract_topology.hpp"

namespace opossum {

/**
 * For the purpose of testing NUMA concepts such as work stealing on a single-node Machine (e.g. development machine),
 * creates one TaskQueue and one Worker per Core.
 */
class FakeNumaTopology : public AbstractTopology {
 public:
  void setup(AbstractScheduler& scheduler) override;
};
}  // namespace opossum
