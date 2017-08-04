#include "job_task.hpp"

namespace opossum {

void JobTask::on_execute() { _fn(); }

}  // namespace opossum
