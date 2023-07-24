#include "shut_down_task.hpp"

namespace hyrise {

void ShutDownTask::_on_execute() {
  --_active_worker_count;
}

}  // namespace hyrise
