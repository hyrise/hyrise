#pragma once

#if HYRISE_NUMA_SUPPORT

#include <chrono>

#include "scheduler/abstract_task.hpp"
#include "storage/numa_placement_manager.hpp"

namespace opossum {

class MigrationPreparationTask : public AbstractTask {
 public:
  explicit MigrationPreparationTask(const NUMAPlacementManager::Options& options);

  static int get_node_id(const PolymorphicAllocator<size_t>& alloc);

 protected:
  void _on_execute() override;

  const NUMAPlacementManager::Options _options;
};
}  // namespace opossum

#endif
