#include "current_scheduler.hpp"

#include <memory>
#include <vector>

#include "abstract_scheduler.hpp"
#include "task_queue.hpp"

namespace opossum {

void CurrentScheduler::set(const std::shared_ptr<AbstractScheduler>& instance) {
  DebugAssert(instance, "You cannot set the scheduler to nullptr - please use NoScheduler instead.");

  _instance->finish();
  _instance = instance;
  _instance->begin();
}

bool CurrentScheduler::is_set() { return !!_instance; }

void CurrentScheduler::wait_for_all_tasks() { _instance->wait_for_all_tasks(); }

}  // namespace opossum
