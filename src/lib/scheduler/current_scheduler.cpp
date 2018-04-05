#include "current_scheduler.hpp"

#include <memory>
#include <vector>

#include "abstract_scheduler.hpp"

namespace opossum {

AbstractSchedulerSPtr CurrentScheduler::_instance;

const AbstractSchedulerSPtr& CurrentScheduler::get() { return _instance; }

void CurrentScheduler::set(const AbstractSchedulerSPtr& instance) {
  if (_instance) _instance->finish();
  _instance = instance;
  if (_instance) _instance->begin();
}

bool CurrentScheduler::is_set() { return !!_instance; }

}  // namespace opossum
