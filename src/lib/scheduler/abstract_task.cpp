#include "abstract_task.hpp"

#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_scheduler.hpp"
#include "current_scheduler.hpp"
#include "task_queue.hpp"
#include "worker.hpp"

#include "utils/assert.hpp"

namespace opossum {

TaskID AbstractTask::id() const { return _id; }

NodeID AbstractTask::node_id() const { return _node_id; }

bool AbstractTask::is_ready() const { return _predecessor_counter == 0; }

bool AbstractTask::is_done() const { return _done; }

bool AbstractTask::is_scheduled() const { return _is_scheduled; }

std::string AbstractTask::description() const {
  return _description.empty() ? "{Task with id: " + std::to_string(_id) + "}" : _description;
}

void AbstractTask::set_id(TaskID id) { _id = id; }

void AbstractTask::set_as_predecessor_of(std::shared_ptr<AbstractTask> successor) {
  DebugAssert((!_is_scheduled), "Possible race: Don't set dependencies after the Task was scheduled");

  successor->_on_predecessor_added();
  _successors.emplace_back(successor);
}

const std::vector<std::shared_ptr<AbstractTask>>& AbstractTask::successors() const { return _successors; }

void AbstractTask::set_node_id(NodeID node_id) { _node_id = node_id; }

bool AbstractTask::try_mark_as_enqueued() { return !_is_enqueued.exchange(true); }

void AbstractTask::set_done_callback(const std::function<void()>& done_callback) {
  DebugAssert((!_is_scheduled), "Possible race: Don't set callback after the Task was scheduled");

  _done_callback = done_callback;
}

void AbstractTask::schedule(NodeID preferred_node_id, SchedulePriority priority) {
  _mark_as_scheduled();

  if (CurrentScheduler::is_set()) {
    CurrentScheduler::get()->schedule(shared_from_this(), preferred_node_id, priority);
  } else {
    // If the Task isn't ready, it will execute() once its dependency counter reaches 0
    if (is_ready()) execute();
  }
}

void AbstractTask::join() {
  DebugAssert((_is_scheduled), "Task must be scheduled before it can be waited for");

  /**
   * When join() is called from a Task, i.e. from a Worker Thread, let the worker handle the join()-ing (via
   * _wait_for_tasks()), otherwise just join right here
   */
  if (CurrentScheduler::is_set()) {
    auto worker = Worker::get_this_thread_worker();
    if (worker) {
      worker->_wait_for_tasks(std::vector<std::shared_ptr<AbstractTask>>({shared_from_this()}));
      return;
    }
  }

  _join_without_replacement_worker();
}

void AbstractTask::_join_without_replacement_worker() {
  std::unique_lock<std::mutex> lock(_done_mutex);
  _done_condition_variable.wait(lock, [&]() { return _done; });

  // An exception was thrown in the task. We caught it there so that the worker does not crash. This might be an
  // exception from a predecessor task. If you get an exception here, a good idea is to disable the scheduler so
  // that it throws immediately. Also, you could use gcc with "catch throw"
  if (_exception) std::rethrow_exception(_exception);
}

void AbstractTask::execute() {
  DebugAssert(!(_started.exchange(true)), "Possible bug: Trying to execute the same task twice");
  DebugAssert(is_ready(), "Task must not be executed before its dependencies are done");

  try {
    _on_execute();
  } catch (...) {
    if (CurrentScheduler::is_set() && Worker::get_this_thread_worker()) {
      // We don't want the worker to die because of an exception. Instead, whoever created the task should deal with it
      _exception = std::current_exception();

      // Someone might be waiting on this. Right now, we don't have a better way of notifying the creator of the task
      // that they will wait forever.
      // TODO introduce exeception callback, make sure it is set befor done_callback is called
      if (_done_callback) {
        throw;
      }

      _mark_as_done();
      for (auto& successor : _successors) {
        successor->_on_exception_in_predecessor(_exception);
      }
      return;
    } else {
      // No worker, so we just throw it right here and now.
      throw;
    }
  }

  for (auto& successor : _successors) {
    successor->_on_predecessor_done();
  }

  if (_done_callback) _done_callback();

  _mark_as_done();
}

void AbstractTask::_mark_as_done() {
  {
    std::unique_lock<std::mutex> lock(_done_mutex);
    _done = true;
  }
  _done_condition_variable.notify_all();
}

void AbstractTask::_mark_as_scheduled() {
  [[gnu::unused]] auto already_scheduled = _is_scheduled.exchange(true);

  DebugAssert((!already_scheduled), "Task was already scheduled!");
}

void AbstractTask::_on_exception_in_predecessor(const std::exception_ptr exception) {
  _exception = exception;
  _mark_as_done();
  for (auto& successor : _successors) {
    successor->_on_exception_in_predecessor(exception);
  }

  if (CurrentScheduler::is_set()) {
    // update number of tasks executed so that Scheduler::finish finds the correct number of executed threads
    auto worker = Worker::get_this_thread_worker();
    worker->processing_unit().lock()->on_worker_finished_task();
  }
}

void AbstractTask::_on_predecessor_added() { _predecessor_counter++; }

void AbstractTask::_on_predecessor_done() {
  auto new_predecessor_count = --_predecessor_counter;  // atomically decrement
  if (new_predecessor_count == 0) {
    if (CurrentScheduler::is_set()) {
      auto worker = Worker::get_this_thread_worker();
      DebugAssert(static_cast<bool>(worker), "No worker");

      worker->queue()->push(shared_from_this(), static_cast<uint32_t>(SchedulePriority::High));
    } else {
      if (_is_scheduled) execute();
      // Otherwise it will get execute()d once it is scheduled. It is entirely possible for Tasks to "become ready"
      // before they are being scheduled in a no-Scheduler context. Think:
      //
      // task1->set_as_predecessor_of(task2);
      // task2->set_as_predecessor_of(task3);
      //
      // task3->schedule(); <-- Does nothing
      // task1->schedule(); <-- Executes Task1, Task2 becomes ready but is not executed, since it is not yet scheduled
      // task2->schedule(); <-- Executes Task2, Task3 becomes ready, executes Task3
    }
  }
}

}  // namespace opossum
