#include "shutdown_task.hpp"

#include "utils/assert.hpp"

namespace hyrise {

void ShutdownTask::_on_execute() {
  const auto remaining_workers = _active_worker_count--;
  Assert(remaining_workers > 0, "Cannot decrement active worker count when no active workers are left.");
}

}  // namespace hyrise
