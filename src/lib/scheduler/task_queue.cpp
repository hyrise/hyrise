#include "task_queue.hpp"

#include <memory>
#include <utility>

namespace opossum {

TaskQueue::TaskQueue(NodeID node_id) : _node_id(node_id) {}

bool TaskQueue::empty() const {
  std::lock_guard<std::mutex> lock(_mutex);

  return _queue.empty();
}

NodeID TaskQueue::node_id() const { return _node_id; }

void TaskQueue::push_back(std::shared_ptr<AbstractTask> task) {
  std::lock_guard<std::mutex> lock(_mutex);

  task->set_node_id(_node_id);
  _queue.emplace_back(std::move(task));
}

void TaskQueue::push_front(std::shared_ptr<AbstractTask> task) {
  std::lock_guard<std::mutex> lock(_mutex);

  task->set_node_id(_node_id);
  _queue.insert(_queue.begin(), std::move(task));
}

std::shared_ptr<AbstractTask> TaskQueue::pull() {
  std::lock_guard<std::mutex> lock(_mutex);

  auto success = false;
  auto index = get_ready_task_index(success);

  if (!success) {
    return nullptr;
  }

  auto task = _queue[index];
  _queue.erase(_queue.begin() + index);

  return task;
}

std::shared_ptr<const AbstractTask> TaskQueue::get_ready_task() {
  std::lock_guard<std::mutex> lock(_mutex);

  auto success = false;
  auto index = get_ready_task_index(success);

  if (!success) {
    return nullptr;
  }

  return _queue[index];
}

std::shared_ptr<AbstractTask> TaskQueue::steal_task(std::shared_ptr<const AbstractTask> task) {
  std::lock_guard<std::mutex> lock(_mutex);

  for (size_t t = 0; t < _queue.size(); t++) {
    if (_queue[t] == task) {
      auto stolen_task = _queue[t];
      _queue.erase(_queue.begin() + t);
      return stolen_task;
    }
  }

  return nullptr;
}

size_t TaskQueue::get_ready_task_index(bool& success) const {
  if (_queue.empty()) {
    success = false;
    return 0;
  }

  for (size_t t = 0; t < _queue.size(); t++) {
    auto task = _queue[t];

    if (task->is_ready()) {
      success = true;
      return t;
    }
  }

  success = false;
  return 0;
}

}  // namespace opossum
