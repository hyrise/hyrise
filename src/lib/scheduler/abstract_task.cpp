#include "abstract_task.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_scheduler.hpp"
#include "hyrise.hpp"
#include "task_queue.hpp"
#include "utils/tracing/probes.hpp"
#include "worker.hpp"

#include "utils/assert.hpp"

namespace opossum {

AbstractTask::AbstractTask(SchedulePriority priority, bool stealable) : _priority(priority), _stealable(stealable) {}

TaskID AbstractTask::id() const { return _id; }

NodeID AbstractTask::node_id() const { return _node_id; }

bool AbstractTask::is_ready() const { return _pending_predecessors == 0; }

bool AbstractTask::is_done() const { return _done; }

bool AbstractTask::is_stealable() const { return _stealable; }

bool AbstractTask::is_scheduled() const { return _is_scheduled; }

std::string AbstractTask::description() const {
  return _description.empty() ? "{Task with id: " + std::to_string(_id) + "}" : _description;
}

void AbstractTask::set_id(TaskID id) { _id = id; }

void AbstractTask::set_as_predecessor_of(const std::shared_ptr<AbstractTask>& successor) {
  Assert((!_is_scheduled), "Possible race: Don't set dependencies after the Task was scheduled");

  successor->_pending_predecessors++;
  _successors.emplace_back(successor);
  successor->_predecessors.emplace_back(shared_from_this());
}

const std::vector<std::weak_ptr<AbstractTask>>& AbstractTask::predecessors() const { return _predecessors; }

const std::vector<std::shared_ptr<AbstractTask>>& AbstractTask::successors() const { return _successors; }

void AbstractTask::set_node_id(NodeID node_id) { _node_id = node_id; }

bool AbstractTask::try_mark_as_enqueued() { return !_is_enqueued.exchange(true); }

void AbstractTask::set_done_callback(const std::function<void()>& done_callback) {
  DebugAssert((!_is_scheduled), "Possible race: Don't set callback after the Task was scheduled");

  _done_callback = done_callback;
}

void AbstractTask::schedule(NodeID preferred_node_id) {
  // We need to make sure that data written by the scheduling thread is visible in the thread executing the task. While
  // spawning a thread is an implicit barrier, we have no such guarantee when we simply add a task to a queue and it is
  // executed by an unrelated thread. Thus, we add a memory barrier.
  //
  // For the other direction (making sure that this task's writes are visible to whoever scheduled it), we have the
  // _done_condition_variable.
  std::atomic_thread_fence(std::memory_order_seq_cst);

  _mark_as_scheduled();

  Hyrise::get().scheduler()->schedule(shared_from_this(), preferred_node_id, _priority);
}

void AbstractTask::_join() {
  DebugAssert(_is_scheduled, "Task must be scheduled before it can be waited for");

  std::unique_lock<std::mutex> lock(_done_mutex);
  _done_condition_variable.wait(lock, [&]() { return static_cast<bool>(_done); });
}

void AbstractTask::execute() {
  DTRACE_PROBE3(HYRISE, JOB_START, _id.load(), _description.c_str(), reinterpret_cast<uintptr_t>(this));
  DebugAssert(!(_started.exchange(true)), "Possible bug: Trying to execute the same task twice");
  DebugAssert(is_ready(), "Task must not be executed before its dependencies are done");

  std::atomic_thread_fence(std::memory_order_seq_cst);  // See documentation in AbstractTask::schedule

  // As tsan does not identify the order imposed by standalone memory fences (as of Oct 2019), we need an atomic
  // read/write combination in whoever scheduled this task and the task itself. As schedule() (in "thread" A) writes to
  // _is_scheduled and this assert (potentially in "thread" B) reads it, it is guaranteed that no writes of whoever
  // spawned the task are pushed down to a point where this thread is already running.
  Assert(_is_scheduled, "Task should be have been scheduled before being executed");

  _on_execute();

  for (auto& successor : _successors) {
    successor->_on_predecessor_done();
  }

  if (_done_callback) _done_callback();

  {
    std::lock_guard<std::mutex> lock(_done_mutex);
    _done = true;
  }
  _done_condition_variable.notify_all();
  DTRACE_PROBE2(HYRISE, JOB_END, _id, reinterpret_cast<uintptr_t>(this));
}

void AbstractTask::_mark_as_scheduled() {
  [[maybe_unused]] auto already_scheduled = _is_scheduled.exchange(true);

  DebugAssert((!already_scheduled), "Task was already scheduled!");
}

void AbstractTask::_on_predecessor_done() {
  auto new_predecessor_count = --_pending_predecessors;  // atomically decrement
  if (new_predecessor_count == 0) {
    auto worker = Worker::get_this_thread_worker();

    if (worker) {
      // If the first task was executed faster than the other tasks were scheduled, we might end up in a situation where
      // the successor is not properly scheduled yet. At the time of writing, this did not make a difference, but for
      // the sake of a clearly defined life cycle, we wait for the task to be scheduled.
      if (!_is_scheduled) return;

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
