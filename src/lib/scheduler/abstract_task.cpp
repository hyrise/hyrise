#include "abstract_task.hpp"

#include <assert.h>
#include <memory>
#include <utility>
#include <vector>

namespace opossum {

TaskID AbstractTask::id() const { return _id; }

NodeID AbstractTask::node_id() const { return _node_id; }

bool AbstractTask::is_ready() const {
  if (_ready) return true;

  for (auto& dependency : _dependencies) {
    if (!dependency->is_done()) {
      return false;
    }
  }

  // Cache that tasks have been completed.
  _ready = true;
  return true;
}

bool AbstractTask::is_done() const { return _done; }

void AbstractTask::set_id(TaskID id) { _id = id; }

void AbstractTask::set_dependencies(std::vector<std::shared_ptr<AbstractTask>>&& dependencies) {
  _dependencies = std::move(dependencies);
}

void AbstractTask::set_node_id(NodeID node_id) { _node_id = node_id; }

void AbstractTask::execute() {
  assert(is_ready());  // Can't execute non-ready task

  on_execute();
  _done = true;
}
}  // namespace opossum
