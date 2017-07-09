#include "current_scheduler.hpp"

#include <memory>
#include <vector>

#include "abstract_task.hpp"

namespace opossum {

std::shared_ptr<AbstractScheduler> CurrentScheduler::_instance;

const std::shared_ptr<AbstractScheduler>& CurrentScheduler::get() { return _instance; }

void CurrentScheduler::set(const std::shared_ptr<AbstractScheduler>& instance) {
  _instance = instance;
  if (_instance) _instance->begin();
}

bool CurrentScheduler::is_set() { return !!_instance; }

}  // namespace opossum
