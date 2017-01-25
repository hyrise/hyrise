#pragma once

#include <memory>
#include <vector>

#include "types.hpp"

namespace opossum {

/**
 * Base class for anything that can be scheduled by the Scheduler and gets executed by a Worker.
 *
 * Derive and implement logic in on_execute()
 */
class AbstractTask {
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

  void execute();

 protected:
  virtual void on_execute() = 0;

 private:
  TaskID _id = INVALID_TASK_ID;
  NodeID _node_id = INVALID_NODE_ID;
  bool _done = false;
  mutable bool _ready = false;
  std::vector<std::shared_ptr<AbstractTask>> _dependencies;
};

}  // namespace opossum
