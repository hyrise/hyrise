#include "shutdown_task.hpp"

namespace hyrise {

void ShutdownTask::_on_execute() {
  --_active_worker_count;
  Assert(_active_worker_count.load() >= 0, "Cannot have less than zero active workers.");
}

}  // namespace hyrise
