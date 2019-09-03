#include "immediate_execution_scheduler.hpp"

namespace opossum {

void ImmediateExecutionScheduler::schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id,
                                           SchedulePriority priority) {
  if (task->is_ready()) task->execute();
}

}  // namespace opossum
