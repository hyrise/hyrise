#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

class Worker;

/**
 * Base class for anything that can be scheduled by the Scheduler and gets executed by a Worker.
 *
 * Derive and implement logic in _on_execute()
 */
class AbstractTask : public std::enable_shared_from_this<AbstractTask> {
  // Using friend classes is quite uncommon in Hyrise. The reason it is done here is that the _join method must
  // absolutely not be called from anyone but the scheduler. As the interface could tempt developers to do so if that
  // method was public, we chose this approach.
  friend class AbstractScheduler;

 public:
  explicit AbstractTask(SchedulePriority priority = SchedulePriority::Default, bool stealable = true);
  virtual ~AbstractTask() = default;

  /**
   * Unique ID of a task. Currently not in use, but really helpful for debugging.
   */
  TaskID id() const;
  NodeID node_id() const;

  /**
   * @return All dependencies are done
   */
  bool is_ready() const;

  /**
   * @return The task finished executing
   */
  bool is_done() const;

  /**
   * @return Workers are allowed to steal the task from another node
   */
  bool is_stealable() const;

  /**
   * Description for debugging purposes
   */
  virtual std::string description() const;

  /**
   * Task ids are determined on scheduling, no one else but the Scheduler should have any reason to call this
   * @param id id, unique during the lifetime of the program, of the task
   */
  void set_id(TaskID id);

  /**
   * Make this Task the dependency of another
   * @param successor Task that will be executed after this
   */
  void set_as_predecessor_of(const std::shared_ptr<AbstractTask>& successor);

  /**
   * @return the predecessors of this Task
   */
  const std::vector<std::weak_ptr<AbstractTask>>& predecessors() const;

  /**
   * @return the successors of this Task
   */
  const std::vector<std::shared_ptr<AbstractTask>>& successors() const;

  /**
   * Node ids are changed when moving the Task between nodes (e.g. during work stealing)
   */
  void set_node_id(NodeID node_id);

  /**
   * Callback to be executed right after the Task finished.
   * Notice the execution of the callback might happen on ANY thread
   */
  void set_done_callback(const std::function<void()>& done_callback);

  /**
   * Schedules the task if a Scheduler is available, otherwise just executes it on the current Thread
   */
  void schedule(NodeID preferred_node_id = CURRENT_NODE_ID);

  /**
   * @returns true when the task was scheduled successfully
   */
  bool is_scheduled() const;

  /**
   * @returns true whether the caller is atomically the first to try to enqueue this task into a TaskQueue,
   * false otherwise.
   * Makes sure a task only gets put into a TaskQueue once
   */
  bool try_mark_as_enqueued();

  /**
   * returns true whether the caller is atomically the first to try to assign this task to a worker,
   * false otherwise.
   */
  bool try_mark_as_assigned_to_worker();

  /**
   * Executes the task in the current Thread, blocks until all operations are finished
   */
  void execute();

 protected:
  virtual void _on_execute() = 0;

 private:
  /**
   * Called by a dependency when it finished execution
   */
  void _on_predecessor_done();

  /**
   * Blocks the calling thread until the Task finished executing.
   * This is only called from non-Worker threads and from Hyrise::get().scheduler()->wait_for_tasks().
   */
  void _join();

  std::atomic<TaskID> _id{INVALID_TASK_ID};
  std::atomic<NodeID> _node_id = INVALID_NODE_ID;
  SchedulePriority _priority;
  std::atomic_bool _stealable;
  std::function<void()> _done_callback;

  // For dependencies
  std::atomic_uint32_t _pending_predecessors{0};
  std::vector<std::weak_ptr<AbstractTask>> _predecessors;
  std::vector<std::shared_ptr<AbstractTask>> _successors;

  // For making sure a task gets only scheduled and enqueued once, respectively
  // A Task is scheduled once schedule() is called and enqueued, which is an internal process, once it has been added
  // to a TaskQueue. Once a worker has chosen to execute this task (and it is thus can no longer be executed by anyone
  // else), _is_assigned_to_worker is set to true.
  enum class TaskState : uint8_t {
    Created = 0,
    Scheduled = 1,
    Enqueued = 2,
    AssignedToWorker = 3,
    Started = 4,
    Done = 5
  };
  std::atomic<TaskState> _state{TaskState::Created};
  bool _try_transition_to(TaskState new_state);
  std::mutex _transition_to_mutex;

  // For making Tasks join()-able
  std::condition_variable _done_condition_variable;
  std::mutex _done_condition_variable_mutex;

  // Purely for debugging purposes, in order to be able to identify tasks after they have been scheduled
  std::string _description;
};

}  // namespace opossum
