#pragma once

#include <stdint.h>
#include <tbb/concurrent_queue.h>
#include <array>
#include <atomic>
#include <condition_variable>
#include <memory>

#include "types.hpp"

namespace opossum {

class AbstractTask;

/**
 * Holds a queue of AbstractTasks, usually one of these exists per node
 */
class TaskQueue {
 public:
  static constexpr uint32_t NUM_PRIORITY_LEVELS = 2;

  explicit TaskQueue(NodeID node_id);

  bool empty() const;

  NodeID node_id() const;

  void push(const std::shared_ptr<AbstractTask>& task, uint32_t priority);

  /**
   * Returns a Tasks that is ready to be executed and removes it from the queue
   */
  std::shared_ptr<AbstractTask> pull();

  /**
   * Returns a Tasks that is ready to be executed and removes it from one of the stealable queues
   */
  std::shared_ptr<AbstractTask> steal();

  /**
   * Notifies one worker as soon as a new task gets pushed into the queue
   */
  std::condition_variable new_task;

  /**
   * Mutex accessed by workers in order to notify them using condition variable
   */
  std::mutex lock;

 private:
  NodeID _node_id;
  std::array<tbb::concurrent_queue<std::shared_ptr<AbstractTask>>, NUM_PRIORITY_LEVELS> _queues;
};

}  // namespace opossum
