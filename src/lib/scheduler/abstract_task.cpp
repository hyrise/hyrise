#include "abstract_task.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "hyrise.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "worker.hpp"

namespace hyrise {

AbstractTask::AbstractTask(SchedulePriority priority, bool stealable) : _priority{priority}, _stealable{stealable} {}

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
  for (const auto& present_successor : _successors) {
    if (&present_successor.get() == &*successor) {
      return;
    }
  }

  _successors.emplace_back(std::ref(*successor));
  successor->_predecessors.emplace_back(std::ref(*this));

  // A task that is already done will not call _on_predecessor_done at the successor. Consequently, the successor's
  // _pending_predecessors count will not decrement. To avoid starvation at the successor, we do not increment its
  // _pending_predecessors count in the first place when this task is already done.
  // Note that _done_condition_variable_mutex must be locked to prevent a race condition where _on_predecessor_done
  // is called before _pending_predecessors++ has executed.
  Assert(!is_scheduled(), "Dependencies of already scheduled task modified.");
  ++successor->_pending_predecessors;
}

const std::vector<std::reference_wrapper<AbstractTask>>& AbstractTask::predecessors() const {
  return _predecessors;
}

const std::vector<std::reference_wrapper<AbstractTask>>& AbstractTask::successors() const {
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
  DebugAssert(!is_scheduled(), "Possible race: Don't set callback after the Task was scheduled.");

  _done_callback = done_callback;
}

void AbstractTask::schedule(NodeID preferred_node_id) {
  // We need to make sure that data written by the scheduling thread is visible in the thread executing the task. While
  // spawning a thread is an implicit barrier, we have no such guarantee when we simply add a task to a queue and it is
  // executed by an unrelated thread. Thus, we add a memory barrier.
  //
  // For the other direction (making sure that this task's writes are visible to whoever scheduled it), we have the
  // _task_done.
  std::atomic_thread_fence(std::memory_order_seq_cst);

  // Atomically marks the task as scheduled or returns if another thread has already scheduled it.
  if (!_try_transition_to(TaskState::Scheduled)) {
    return;
  }

  Hyrise::get().scheduler()->_schedule(shared_from_this(), preferred_node_id, _priority);
}

void AbstractTask::_join() {
  if (is_done()) {
    return;
  }

  DebugAssert(is_scheduled(), "Task must be scheduled before it can be waited for.");
  _task_done.wait(false);
  Assert(is_done(), "Unexpected state of Done.");
}

void AbstractTask::execute() {
  {
    const auto success_started = _try_transition_to(TaskState::Started);
    Assert(success_started, "Expected successful transition to TaskState::Started.");
  }
  DebugAssert(is_ready(), "Task must not be executed before its dependencies are done.");

  std::atomic_thread_fence(std::memory_order_seq_cst);  // See documentation in AbstractTask::schedule

  // As tsan does not identify the order imposed by standalone memory fences (as of Oct 2019), we need an atomic
  // read/write combination in whoever scheduled this task and the task itself. As schedule() (in "thread" A) writes to
  // _is_scheduled and this assert (potentially in "thread" B) reads it, it is guaranteed that no writes of whoever
  // spawned the task are pushed down to a point where this thread is already running.

  _on_execute();

  for (auto& successor : _successors) {
    // macOS silently ignores non-reachable successors (see `SuccessorExpired` test). Thus, we obtain a shared pointer
    // here which also causes macOS to recognize that the successor is not accessible (note, the following line fails
    // with an std::bad_weak_ptr exception before testing the assertion).
    DebugAssert(successor.get().shared_from_this(), "Cannot obtain successor.");

    // The task creator is responsible to ensure that successor tasks are available whenever an executed task tries to
    // execute/accesss its successors.
    successor.get()._on_predecessor_done();
  }

  {
    // We set the task's state to done after informing all successors. It can happen that a successor (that
    // cannot be executed until its predessors are done) is both scheduled and pulled by a worker and at the same time
    // executed here by the current worker.
    // Note, informing successors does not block the current task (unless we use the ImmediateExecutionScheduler).
    const auto success_done = _try_transition_to(TaskState::Done);
    Assert(success_done, "Expected successful transition to TaskState::Done.");
  }

  if (_done_callback) {
    _done_callback();
  }

  _task_done.store(true);
  _task_done.notify_all();
}

TaskState AbstractTask::state() const {
  return _state;
}

void AbstractTask::_on_predecessor_done() {
  const auto previous_predecessor_count = _pending_predecessors--;
  Assert(previous_predecessor_count > 0, "Cannot decrement pending predecessors when no predecessors are left.");
  if (previous_predecessor_count == 1) {
    const auto current_worker = Worker::get_this_thread_worker();

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
  const auto lock = std::lock_guard<std::mutex>{_transition_to_mutex};
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
      Assert(TaskState::Scheduled, "Illegal state transition to TaskState::Enqueued.");
      break;
    case TaskState::AssignedToWorker:
      if (_state >= TaskState::AssignedToWorker) {
        return false;
      }
      Assert(_state == TaskState::Scheduled || _state == TaskState::Enqueued,
             "Illegal state transition to TaskState::AssignedToWorker.");
      break;
    case TaskState::Started:
      Assert(_state == TaskState::Scheduled || _state == TaskState::AssignedToWorker,
             "Illegal state transition to TaskState::Started: Task should have been scheduled before being executed.");
      break;
    case TaskState::Done:
      Assert(_state == TaskState::Started, "Illegal state transition to TaskState::Done.");
      break;
    default:
      Fail("Unexpected target state in AbstractTask.");
  }

  _state.exchange(new_state);
  return true;
}

}  // namespace hyrise
