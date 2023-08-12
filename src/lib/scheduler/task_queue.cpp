#include "task_queue.hpp"

#include <memory>
#include <utility>

#include "abstract_task.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TaskQueue::TaskQueue(NodeID node_id) : _node_id(node_id) {}

bool TaskQueue::empty() const {
  for (const auto& queue : _queues) {
    if (!queue.empty()) {
      return false;
    }
  }
  return true;
}

NodeID TaskQueue::node_id() const {
  return _node_id;
}

void TaskQueue::push(const std::shared_ptr<AbstractTask>& task, const SchedulePriority priority) {
  const auto priority_uint = static_cast<uint32_t>(priority);
  DebugAssert(priority_uint < NUM_PRIORITY_LEVELS, "Illegal priority level");

  // Someone else was first to enqueue this task? No problem!
  if (!task->try_mark_as_enqueued()) {
    return;
  }

  task->set_node_id(_node_id);
  _queues[priority_uint].push(task);

  new_task.notify_one();
}

std::shared_ptr<AbstractTask> TaskQueue::pull() {
  std::shared_ptr<AbstractTask> task;
  for (auto& queue : _queues) {
    if (queue.try_pop(task)) {
      return task;
    }
  }
  return nullptr;
}

std::shared_ptr<AbstractTask> TaskQueue::steal() {
  std::shared_ptr<AbstractTask> task;
  for (auto& queue : _queues) {
    if (queue.try_pop(task)) {
      if (task->is_stealable()) {
        return task;
      }

      queue.push(task);
    }
  }
  return nullptr;
}

size_t TaskQueue::estimate_load() {
  auto estimated_load = size_t{0};

  // Simple heuristic to estimate the load: the higher the priority, the higher the costs. We use powers of two
  // (starting with 2^0) to calculate the cost factor per priority level.
  for (auto queue_id = size_t{0}; queue_id < NUM_PRIORITY_LEVELS; ++queue_id) {
    // The lowest priority has a multiplier of 2^0, the next higher priority 2^1, and so on.
    estimated_load += _queues[queue_id].unsafe_size() * (size_t{1} << (NUM_PRIORITY_LEVELS - 1 - queue_id));
  }

  return estimated_load;
}

}  // namespace hyrise
