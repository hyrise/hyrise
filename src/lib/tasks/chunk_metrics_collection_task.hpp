#pragma once

#include "scheduler/abstract_task.hpp"

namespace opossum {

class ChunkMetricsCollectionTask : public AbstractTask {
 public:
  ChunkMetricsCollectionTask();

 protected:
  void _on_execute() override;
};

}  // namespace opossum
