#pragma once

#if OPOSSUM_NUMA_SUPPORT

#include "abstract_topology.hpp"

namespace opossum {

/**
 * Creates a topology with one queue per node and one worker per CPU
 */
class NumaTopology : public AbstractTopology {
 public:
  void setup(AbstractScheduler& scheduler) override;
};
}  // namespace opossum

#endif
