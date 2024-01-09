#pragma once

#include <stdint.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <memory>

#include <tbb/concurrent_queue.h>  // NOLINT(build/include_order): wronlgy identified as a C header.

#include "concurrentqueue.h"  // The lightweight semaphore uses definitions of concurrentqueue.h.
#include "lightweightsemaphore.h"

#include "types.hpp"

namespace moodycamel {  //
class LightweightSemaphore;
}

namespace hyrise {

class AbstractTask;

/**
 * Holds a queue of AbstractTasks, usually one of these exists per node
 */
class TaskQueue {
 public:
  static constexpr uint32_t NUM_PRIORITY_LEVELS = 2;

  TaskQueue() = delete;

  explicit TaskQueue(NodeID node_id);

  bool empty() const;

  NodeID node_id() const;

  void push(const std::shared_ptr<AbstractTask>& task, const SchedulePriority priority);

  /**
   * Returns a Tasks that is ready to be executed and removes it from the queue
   */
  std::shared_ptr<AbstractTask> pull();

  /**
   * Returns a Tasks that is ready to be executed and removes it from one of the stealable queues
   */
  std::shared_ptr<AbstractTask> steal();

  /**
   * Returns the estimated load for the TaskQueue (i.e., all queues of the TaskQueue instance). The load is "estimated"
   * as TBB's concurrent queue does not guarantee that `unsafe_size()` returns the correct size at a given point in
   * time. The priority queues are weighted, i.e., a task in the high priority queue leads to a larger load than a task
   * in the default priority queue.
   */
  size_t estimate_load() const;

  void signal(const size_t count);

  /**
   * Semaphore to signal waiting workers for new tasks.
   */
  moodycamel::LightweightSemaphore semaphore;

 private:
  NodeID _node_id{INVALID_NODE_ID};
  std::array<tbb::concurrent_queue<std::shared_ptr<AbstractTask>>, NUM_PRIORITY_LEVELS> _queues;
};

}  // namespace hyrise
