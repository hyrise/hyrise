#include "current_scheduler.hpp"

#include <memory>
#include <vector>

#include "abstract_task.hpp"
#include "utils/assert.hpp"
#include "worker.hpp"

namespace opossum {

std::shared_ptr<AbstractScheduler> CurrentScheduler::_instance;

const std::shared_ptr<AbstractScheduler>& CurrentScheduler::get() { return _instance; }

void CurrentScheduler::set(const std::shared_ptr<AbstractScheduler>& instance) {
  _instance = instance;
  if (_instance) _instance->begin();
}

bool CurrentScheduler::is_set() { return !!_instance; }

void CurrentScheduler::wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
#if IS_DEBUG
  for (auto& task : tasks) {
    if (!task->is_scheduled()) {
      DebugFail("Schedule tasks before joining them");
    }
  }
#endif

  /**
   * In case wait_for_tasks() is called from a Task being executed in a Worker, block that worker, otherwise just
   * join the tasks
   */
  auto worker = Worker::get_this_thread_worker();
  if (worker) {
    worker->_wait_for_tasks(tasks);
  } else {
    for (auto& task : tasks) task->join();
  }
}
}  // namespace opossum
