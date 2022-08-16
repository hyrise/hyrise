#include "abstract_task.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_scheduler.hpp"
#include "hyrise.hpp"
#include "task_queue.hpp"
#include "worker.hpp"

#include "utils/assert.hpp"

namespace hyrise {

AbstractTask::AbstractTask(SchedulePriority priority, bool stealable) : _priority(priority), _stealable(stealable) {}

TaskID AbstractTask::id() const {
  return _id;
}

NodeID AbstractTask::node_id() const {
  return _node_id;
}

bool AbstractTask::is_ready() const {
  return _pending_predecessors == 0;
}

bool AbstractTask::is_done() const {
  return _state == TaskState::Done;
}

bool AbstractTask::is_stealable() const {
  return _stealable;
}

bool AbstractTask::is_scheduled() const {
  return _state >= TaskState::Scheduled;
}

std::string AbstractTask::description() const {
  return _description.empty() ? "{Task with id: " + std::to_string(_id.load()) + "}" : _description;
}

void AbstractTask::set_id(TaskID task_id) {
  _id = task_id;
}

void AbstractTask::set_as_predecessor_of(const std::shared_ptr<AbstractTask>& successor) {
  // Since OperatorTasks can be reused by, e.g., uncorrelated subqueries, this function may already have been called
  // with the given successor (compare discussion https://github.com/hyrise/hyrise/pull/2340#discussion_r602174096).
  // The following guard prevents adding duplicate successors/predecessors:
  if (std::find(_successors.cbegin(), _successors.cend(), successor) != _successors.cend()) {
    return;
  }

  _successors.emplace_back(successor);
  successor->_predecessors.emplace_back(shared_from_this());

  // A task that is already done will not call _on_predecessor_done at the successor. Consequently, the successor's
  // _pending_predecessors count will not decrement. To avoid starvation at the successor, we do not increment its
  // _pending_predecessors count in the first place when this task is already done.
  // Note that _done_condition_variable_mutex must be locked to prevent a race condition where _on_predecessor_done
  // is called before _pending_predecessors++ has executed.
  std::lock_guard<std::mutex> lock(_done_condition_variable_mutex);
  if (!is_done()) {
    successor->_pending_predecessors++;
  }
}

const std::vector<std::weak_ptr<AbstractTask>>& AbstractTask::predecessors() const {
  return _predecessors;
}

const std::vector<std::shared_ptr<AbstractTask>>& AbstractTask::successors() const {
  return _successors;
}

void AbstractTask::set_node_id(NodeID node_id) {
  _node_id = node_id;
}

bool AbstractTask::try_mark_as_enqueued() {
  return _try_transition_to(TaskState::Enqueued);
}

bool AbstractTask::try_mark_as_assigned_to_worker() {
  return _try_transition_to(TaskState::AssignedToWorker);
}

void AbstractTask::set_done_callback(const std::function<void()>& done_callback) {
  DebugAssert(!is_scheduled(), "Possible race: Don't set callback after the Task was scheduled");

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

  // Atomically marks the task as scheduled or returns if another thread has already scheduled it.
  if (!_try_transition_to(TaskState::Scheduled)) {
    return;
  }

  Hyrise::get().scheduler()->schedule(shared_from_this(), preferred_node_id, _priority);
}

void AbstractTask::_join() {
  auto lock = std::unique_lock<std::mutex>(_done_condition_variable_mutex);
  if (is_done()) {
    return;
  }

  DebugAssert(is_scheduled(), "Task must be scheduled before it can be waited for");
  _done_condition_variable.wait(lock, [&]() { return is_done(); });
}

void AbstractTask::execute() {
  {
    auto success_started = _try_transition_to(TaskState::Started);
    Assert(success_started, "Expected successful transition to TaskState::Started.");
  }
  DebugAssert(is_ready(), "Task must not be executed before its dependencies are done");

  std::atomic_thread_fence(std::memory_order_seq_cst);  // See documentation in AbstractTask::schedule

  // As tsan does not identify the order imposed by standalone memory fences (as of Oct 2019), we need an atomic
  // read/write combination in whoever scheduled this task and the task itself. As schedule() (in "thread" A) writes to
  // _is_scheduled and this assert (potentially in "thread" B) reads it, it is guaranteed that no writes of whoever
  // spawned the task are pushed down to a point where this thread is already running.

  _on_execute();

  {
    auto success_done = _try_transition_to(TaskState::Done);
    Assert(success_done, "Expected successful transition to TaskState::Done.");
  }

  for (auto& successor : _successors) {
    successor->_on_predecessor_done();
  }

  if (_done_callback) {
    _done_callback();
  }

  {
    std::lock_guard<std::mutex> lock(_done_condition_variable_mutex);
    _done_condition_variable.notify_all();
  }
}

TaskState AbstractTask::state() const {
  return _state;
}

void AbstractTask::_on_predecessor_done() {
  Assert(_pending_predecessors > 0, "The count of pending predecessors equals zero and cannot be decremented.");
  auto new_predecessor_count = --_pending_predecessors;  // atomically decrement
  if (new_predecessor_count == 0) {
    auto current_worker = Worker::get_this_thread_worker();

    if (current_worker) {
      // If the first task was executed faster than the other tasks were scheduled, we might end up in a situation where
      // the successor is not properly scheduled yet. At the time of writing, this did not make a difference, but for
      // the sake of a clearly defined lifecycle, we wait for the task to be scheduled.
      if (!is_scheduled()) {
        return;
      }

      // Instead of adding the current task to the queue, try to execute it immediately on the same worker as the last
      // predecessor. This should improve cache locality and reduce the scheduling costs.
      current_worker->execute_next(shared_from_this());
    } else {
      if (is_scheduled()) {
        execute();
      }
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

bool AbstractTask::_try_transition_to(TaskState new_state) {
  /**
   * Before switching state, the validity of the transition must be checked:
   *  a) If the transition is illegal or unexpected, this function fails.
   *  b) If another thread was faster and the state has already progressed to, e.g., TaskState::Scheduled, this
   *     function returns false.
   *
   * This function must be locked to prevent race conditions. A different thread might be able to change _state
   * successfully while this thread is still between the validity check and _state.exchange(new_state).
   */
  std::lock_guard<std::mutex> lock(_transition_to_mutex);
  switch (new_state) {
    case TaskState::Scheduled:
      if (_state >= TaskState::Scheduled) {
        return false;
      }
      Assert(_state == TaskState::Created, "Illegal state transition to TaskState::Scheduled.");
      break;
    case TaskState::Enqueued:
      if (_state >= TaskState::Enqueued) {
        return false;
      }
      Assert(TaskState::Scheduled, "Illegal state transition to TaskState::Enqueued");
      break;
    case TaskState::AssignedToWorker:
      if (_state >= TaskState::AssignedToWorker) {
        return false;
      }
      Assert(_state == TaskState::Scheduled || _state == TaskState::Enqueued,
             "Illegal state transition to TaskState::AssignedToWorker");
      break;
    case TaskState::Started:
      Assert(_state == TaskState::Scheduled || _state == TaskState::AssignedToWorker,
             "Illegal state transition to TaskState::Started: Task should have been scheduled before being executed.");
      break;
    case TaskState::Done:
      Assert(_state == TaskState::Started, "Illegal state transition to TaskState::Done");
      break;
    default:
      Fail("Unexpected target state in AbstractTask.");
  }

  _state.exchange(new_state);
  return true;
}

}  // namespace hyrise
