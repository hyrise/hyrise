#include "job_task.hpp"

#include "worker.hpp"

namespace opossum {

void JobTask::on_execute() { _fn(); }
}
