#pragma once

#if HYRISE_NUMA_SUPPORT

#include "scheduler/abstract_task.hpp"

namespace opossum {

class ChunkMetricsCollectionTask : public AbstractTask {
 public:
  ChunkMetricsCollectionTask() = default;

 protected:
  void _on_execute() override;
};

}  // namespace opossum

#endif
