#include "job_task.hpp"

namespace opossum {

void JobTask::schedule(NodeID preferred_node_id, SchedulePriority priority) {
  AbstractTask::schedule(preferred_node_id, priority);
}

void JobTask::_on_execute() { _fn(); }

}  // namespace opossum
