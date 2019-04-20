#include "current_scheduler.hpp"

#include <memory>
#include <vector>

#include "abstract_scheduler.hpp"

namespace opossum {

std::shared_ptr<AbstractScheduler> CurrentScheduler::_instance;  // NOLINT

void CurrentScheduler::set(const std::shared_ptr<AbstractScheduler>& instance) {
  if (_instance) _instance->finish();
  _instance = instance;
  if (_instance) _instance->begin();
}

bool CurrentScheduler::is_set() { return !!_instance; }

void CurrentScheduler::wait_for_all_tasks() {
  if (_instance) _instance->wait_for_all_tasks();
}

bool CurrentScheduler::has_pending_tasks() {
  if (!_instance) return false;
  for (const auto& queue : _instance->queues) {
    if (!queue->empty()) return true;
  }
  return false;
}

}  // namespace opossum
