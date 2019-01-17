#include "job_task.hpp"

namespace opossum {

void JobTask::_on_execute() { _fn(); }

}  // namespace opossum
