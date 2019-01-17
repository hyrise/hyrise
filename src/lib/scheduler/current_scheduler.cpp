#include "current_scheduler.hpp"  // NEEDEDINCLUDE

#include "abstract_scheduler.hpp"  // NEEDEDINCLUDE

namespace opossum {

std::shared_ptr<AbstractScheduler> CurrentScheduler::_instance;  // NOLINT

void CurrentScheduler::set(const std::shared_ptr<AbstractScheduler>& instance) {
  if (_instance) _instance->finish();
  _instance = instance;
  if (_instance) _instance->begin();
}

bool CurrentScheduler::is_set() { return !!_instance; }

}  // namespace opossum
