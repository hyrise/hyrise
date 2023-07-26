#include "shutdown_task.hpp"

namespace hyrise {

void ShutdownTask::_on_execute() {
  Assert(_active_worker_count.load() > 0, "Cannot have less than zero active workers.");
  --_active_worker_count;
}

}  // namespace hyrise
