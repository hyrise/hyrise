#pragma once

#if HYRISE_NUMA_SUPPORT

#include "scheduler/abstract_task.hpp"         // NEEDEDINCLUDE
#include "storage/numa_placement_manager.hpp"  // NEEDEDINCLUDE
#include "utils/polymorphic_allocator.hpp"

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
