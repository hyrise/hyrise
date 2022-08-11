#include "job_task.hpp"

namespace hyrise {

void JobTask::_on_execute() {
  _fn();
}

}  // namespace hyrise
