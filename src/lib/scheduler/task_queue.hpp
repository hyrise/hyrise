#pragma once

#include <stdint.h>

#include <memory>
#include <mutex>
#include <vector>

#include "abstract_task.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Holds a queue of AbstractTasks, usually one of these exists per node
 */
class TaskQueue {
 public:
  explicit TaskQueue(NodeID node_id);

  bool empty() const;

  NodeID node_id() const;

  void push(std::shared_ptr<AbstractTask> task);

  /**
   * Returns a Tasks that is ready to be executed and removes it from the queue
   */
  std::shared_ptr<AbstractTask> pull();

  /**
   * Returns a Tasks that is ready to be executed but does NOT remove it from the queue
   */
  std::shared_ptr<const AbstractTask> get_ready_task();

  /**
   * Remove @task from this queue, possibly to move it to another queue during work stealing
   */
  std::shared_ptr<AbstractTask> steal_task(std::shared_ptr<const AbstractTask> task);

 private:
  size_t get_ready_task_index(bool& success) const;

 private:
  NodeID _node_id;
  std::vector<std::shared_ptr<AbstractTask>> _queue;
  mutable std::mutex _mutex;
};

}  // namespace opossum
