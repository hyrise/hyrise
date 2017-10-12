#pragma once

#if OPOSSUM_NUMA_SUPPORT

#include <chrono>

#include "scheduler/abstract_task.hpp"
#include "storage/numa_placement_manager.hpp"

namespace opossum {

class MigrationPreparationTask : public AbstractTask {
 public:
  explicit MigrationPreparationTask(const NUMAPlacementManagerOptions& options);

 protected:
  void _on_execute() override;

  const NUMAPlacementManagerOptions _options;
};
}  // namespace opossum

#endif
