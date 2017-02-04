#pragma once

#include <atomic>
#include <condition_variable>
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
 * Derive and implement logic in on_execute()
 */
class AbstractTask : public std::enable_shared_from_this<AbstractTask> {
  friend class Worker;

 public:
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
   * Description for debugging purposes
   */
  std::string description() const;
  void set_description(const std::string& description);

  /**
   * Task ids are determined on scheduling, no one else but the Scheduler should have any reason to call this
   * @param id id, unique during the lifetime of the program, of the task
   */
  void set_id(TaskID id);

  /**
   * @param A set of Tasks that need to finish before this task can be executed
   */
  void set_dependencies(std::vector<std::shared_ptr<AbstractTask>>&& dependencies);

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
  void schedule(NodeID preferred_node_id = CURRENT_NODE_ID, SchedulePriority priority = SchedulePriority::Normal);

  /**
   * Waits for the Task to finish
   * If the Task is being executed on a Worker, allow another Worker to run on the CPU while waiting for this task to
   * finish
   */
  void join();

  /*
   * Atomically marks the Task as scheduled, thus making sure this happens only once
   */
  void mark_as_scheduled();
  bool is_scheduled() const { return _is_scheduled; }

  /**
   * Executes the task in the current Thread, blocks until all operations are finished
   */
  void execute();

 protected:
  virtual void on_execute() = 0;

 private:
  /**
   * Blocks the calling thread until the Task finished executing
   */
  void _join_without_replacement_worker();

  TaskID _id = INVALID_TASK_ID;
  NodeID _node_id = INVALID_NODE_ID;
  bool _done = false;
  mutable bool _ready = false;
  std::vector<std::shared_ptr<AbstractTask>> _dependencies;
  std::function<void()> _done_callback;

  // For making sure a task gets only scheduled once
  std::atomic_bool _is_scheduled{false};

  // For making Tasks join()-able
  std::condition_variable _done_condition_variable;
  std::mutex _done_mutex;

  // Purely for debugging purposes, in order to be able to identify tasks after they have been scheduled
  std::string _description;

#if IS_DEBUG
  // To make sure a task is never executed twice
  std::atomic_bool _started{false};
#endif
};

}  // namespace opossum
