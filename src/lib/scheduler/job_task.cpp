#include "job_task.hpp"

namespace hyrise {

void JobTask::now() {
  // std::printf("NOW\n");
  _fn();
}

void JobTask::_on_execute() {
  _fn();
}

}  // namespace hyrise
