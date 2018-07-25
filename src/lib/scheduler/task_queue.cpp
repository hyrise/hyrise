#include "task_queue.hpp"

#include <memory>
#include <utility>

#include "abstract_task.hpp"
#include "utils/assert.hpp"

namespace opossum {

TaskQueue::TaskQueue(NodeID node_id) : _node_id(node_id) {}

bool TaskQueue::empty() const { return _num_tasks == 0; }

NodeID TaskQueue::node_id() const { return _node_id; }

void TaskQueue::push(const std::shared_ptr<AbstractTask>& task, uint32_t priority) {
  DebugAssert((priority < NUM_PRIORITY_LEVELS), "Illegal priority level");

  // Someone else was first to enqueue this task? No problem!
  if (!task->try_mark_as_enqueued()) return;

  task->set_node_id(_node_id);
  _queues[priority].push(task);

  _num_tasks++;
}

std::shared_ptr<AbstractTask> TaskQueue::pull() {
  std::shared_ptr<AbstractTask> task;
  for (auto& queue : _queues) {
    queue.try_pop(task);

    if (task) {
      _num_tasks--;
      return task;
    }
  }
  return nullptr;
}

std::shared_ptr<AbstractTask> TaskQueue::steal() {
  std::shared_ptr<AbstractTask> task;
  for (auto i : {SchedulePriority::High, SchedulePriority::Normal}) {
    auto& queue = _queues[static_cast<uint32_t>(i)];
    queue.try_pop(task);

    if (task) {
      _num_tasks--;
      return task;
    }
  }
  return nullptr;
}

}  // namespace opossum
