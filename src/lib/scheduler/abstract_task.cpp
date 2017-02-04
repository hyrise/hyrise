#include "abstract_task.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_scheduler.hpp"
#include "current_scheduler.hpp"
#include "worker.hpp"

namespace opossum {

TaskID AbstractTask::id() const { return _id; }

NodeID AbstractTask::node_id() const { return _node_id; }

bool AbstractTask::is_ready() const {
  if (_ready) return true;

  for (auto& dependency : _dependencies) {
    if (!dependency->is_done()) {
      return false;
    }
  }

  // Cache that tasks have been completed.
  _ready = true;
  return true;
}

bool AbstractTask::is_done() const { return _done; }

std::string AbstractTask::description() const {
  return _description.empty() ? "{Task with id: " + std::to_string(_id) + "}" : _description;
}

void AbstractTask::set_description(const std::string& description) {
  if (IS_DEBUG && _is_scheduled) {
    throw std::logic_error("Possible race: Don't set description after the Task was scheduled");
  }

  _description = description;
}

void AbstractTask::set_id(TaskID id) { _id = id; }

void AbstractTask::set_dependencies(std::vector<std::shared_ptr<AbstractTask>>&& dependencies) {
  _dependencies = std::move(dependencies);
}

void AbstractTask::set_node_id(NodeID node_id) { _node_id = node_id; }

void AbstractTask::mark_as_scheduled() {
  auto already_scheduled = _is_scheduled.exchange(true);

  if (IS_DEBUG && already_scheduled) {
    throw std::logic_error("Task was already scheduled!");
  }
}

void AbstractTask::set_done_callback(const std::function<void()>& done_callback) {
  if (IS_DEBUG && _is_scheduled) {
    throw std::logic_error("Possible race: Don't set callback after the Task was scheduled");
  }

  _done_callback = done_callback;
}

void AbstractTask::schedule(NodeID preferred_node_id, SchedulePriority priority) {
  mark_as_scheduled();

  if (CurrentScheduler::is_set()) {
    CurrentScheduler::get()->schedule(shared_from_this(), preferred_node_id, priority);
  } else {
    execute();
  }
}

void AbstractTask::join() {
  if (IS_DEBUG && !_is_scheduled) {
    throw std::logic_error("Task must be scheduled before it can be waited for");
  }

  /**
   * When join() is called from a Task, i.e. from a Worker Thread, let the worker handle the join()-ing (via
   * _wait_for_tasks()), otherwise just join right here
   */
  auto worker = Worker::get_this_thread_worker();
  if (worker) {
    worker->_wait_for_tasks({shared_from_this()});
  } else {
    _join_without_replacement_worker();
  }
}

void AbstractTask::_join_without_replacement_worker() {
  std::unique_lock<std::mutex> lock(_done_mutex);
  _done_condition_variable.wait(lock, [&]() { return _done; });
}

void AbstractTask::execute() {
#if IS_DEBUG
  auto already_started = _started.exchange(true);
  if (already_started) {
    throw std::logic_error("Possible bug: Trying to execute the same task twice");
  }

  if (!is_ready()) {
    throw std::logic_error("Task must not be executed before its dependencies are done");
  }
#endif

  on_execute();

  if (_done_callback) _done_callback();

  {
    std::unique_lock<std::mutex> lock(_done_mutex);
    _done = true;
  }
  _done_condition_variable.notify_all();
}

}  // namespace opossum
