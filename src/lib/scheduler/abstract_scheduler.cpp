#include "abstract_scheduler.hpp"

namespace hyrise {

void AbstractScheduler::wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  DebugAssert(([&]() {
                for (const auto& task : tasks) {
                  if (!task->is_scheduled()) {
                    return false;
                  }
                }
                return true;
              }()),
              "In order to wait for a task’s completion, it needs to have been scheduled first.");

  // In case wait_for_tasks() is called from a Task being executed in a Worker, let the Worker handle the join()-ing,
  // otherwise join right here
  const auto worker = Worker::get_this_thread_worker();
  if (worker) {
    worker->_wait_for_tasks(tasks);
  } else {
    for (const auto& task : tasks) {
      task->_join();
    }
  }
}

void AbstractScheduler::_group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  // Do nothing - grouping tasks is implementation-defined
}

void AbstractScheduler::schedule_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  for (const auto& task : tasks) {
    task->schedule();
  }
}

void AbstractScheduler::schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  _group_tasks(tasks);
  schedule_tasks(tasks);
  wait_for_tasks(tasks);
}

}  // namespace hyrise
